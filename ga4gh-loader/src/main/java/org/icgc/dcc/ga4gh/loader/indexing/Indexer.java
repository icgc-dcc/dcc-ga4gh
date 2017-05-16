package org.icgc.dcc.ga4gh.loader.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.json.JacksonFactory;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.utils.counting.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.id.impl.VariantIdContext;
import org.icgc.dcc.ga4gh.loader.utils.idstorage.storage.MapStorage;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;

@Slf4j
@RequiredArgsConstructor
public class Indexer {

  /**
   * Constants.
   */
  public static final String INDEX_SETTINGS_JSON_FILENAME = "index.settings.json";
  public static final String VCF_HEADER_TYPE_NAME = "vcf_header";
  private static final ObjectWriter BINARY_WRITER = JacksonFactory.getObjectWriter();
  public static final String DEFAULT_MAPPINGS_DIRNAME = "org/icgc/dcc/ga4gh/resources/mappings";
  public static final String DEFAULT_MAPPING_JSON_EXTENSION = ".mapping.json";

  private static final int MAX_NUM_SEGMENTS = 1;

  /**
   * Dependencies.
   */
  @NonNull
  private final Client client;
  @NonNull
  private final DocumentWriter writer;

  /**
   * Configuration.
   */
  @NonNull
  private final IndexCreatorContext indexCreatorContext;

  /*
   * State
   */
  // Keys are strings NAMES, since those should never collide
  @NonNull private final EsVariantSetConverterJson variantSetConverter;
  @NonNull private final EsCallSetConverterJson esCallSetConverter;
  @NonNull private final EsVariantCallPairConverterJson variantCallPairConverter;


  @NonFinal
  private IndexCreator indexCreator = null;


  private final CounterMonitor variantMonitor = CounterMonitor.createCounterMonitor("VariantIndexing", Config.MONITOR_INTERVAL_COUNT);
  private final CounterMonitor vcfHeaderMonitor = CounterMonitor.createCounterMonitor("VCFHeaderIndexing", Config.MONITOR_INTERVAL_COUNT);

  @SneakyThrows
  public void prepareIndex() {
      lazyInitIndexCreator();
      indexCreator.execute();
  }

  private void lazyInitIndexCreator(){
    if (indexCreator == null){
      indexCreator = new IndexCreator(indexCreatorContext);
    }
  }

  public void optimize(){
    lazyInitIndexCreator();
    forceMergeAllIndices(MAX_NUM_SEGMENTS);
    flushAllIndices();
  }

  private void flushAllIndices(){
    log.info("Started paranoid flush...");
    indexCreatorContext.getClient()
    .admin().indices()
    .prepareFlush()
    .execute();
    log.info("Finished flush");
  }

  @SneakyThrows
  private void  forceMergeAllIndices(final int maxNumSegments){
    log.info("Starting force merge on all indices...");
    val client = indexCreatorContext.getClient();
    val request = client.admin().indices().prepareForceMerge().setMaxNumSegments(maxNumSegments);
    val response  = request.execute().get();
    int numFailedShards = response.getFailedShards();
    checkState(numFailedShards==0, "The request to forceMerge all indices to {} segments, had {} failed shards", maxNumSegments, numFailedShards);
    log.info("Finished force merge on all indices");
  }


  private <K, ID> void indexMapStorage(MapStorage<K, ID> mapStorage, BiConsumer<K, ID> consumer){
    mapStorage.getMap().forEach(consumer);
  }

  @SneakyThrows
  public void indexVariants(@NonNull Stream<VariantIdContext<Long>> stream){
    variantMonitor.start();
    stream.forEach(this::writeVariant);
    variantMonitor.stop();
  }

  @SneakyThrows
  public void indexVariantSets(@NonNull MapStorage<EsVariantSet, Integer> variantSetMapStorage) {
    indexMapStorage(variantSetMapStorage, this::writeVariantSet);
  }

  @SneakyThrows
  public void indexCallSets(@NonNull MapStorage<EsCallSet, Integer> callSetMapStorage) {
    indexMapStorage(callSetMapStorage, this::writeCallSet);
  }

  @SneakyThrows
  private void writeVariant(VariantIdContext<Long> variantIdContext){
    val variantId = variantIdContext.getId();
    val esVariantCallPair = variantIdContext.getEsVariantCallPair();
    val data = variantCallPairConverter.convertToObjectNode(esVariantCallPair);
    writer.write(new IndexDocument(variantId.toString(), data, new VariantDocumentType()));
    variantMonitor.preIncr();
  }

  @SneakyThrows
  private void writeCallSet(@NonNull EsCallSet callSet, @NonNull Integer callSetId) {
    val data = esCallSetConverter.convertToObjectNode(callSet);
    writer.write( new IndexDocument(callSetId.toString(),data, new CallSetDocumentType()));
  }

  @SneakyThrows
  private void writeVariantSet(@NonNull EsVariantSet variantSet, @NonNull Integer variantSetId) {
    val data = variantSetConverter.convertToObjectNode(variantSet);
    writer.write(new IndexDocument(variantSetId.toString(), data, new VariantSetDocumentType()));
  }

  private static byte[] createSource(@NonNull final Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  // TODO: [rtisma] rethink how will organize this dao
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    val parent_variant_set_id = vcfHeader.path(VARIANT_SET_ID).textValue();
    checkState(
        client.prepareIndex(Config.PARENT_CHILD_INDEX_NAME, VCF_HEADER, objectId)
            .setContentType(SMILE)
            .setSource(createSource(vcfHeader))
            .setParent(parent_variant_set_id)
            .setRouting(VARIANT)
            .get().status().equals(RestStatus.CREATED));
  }


  private static class VariantDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT;
    }
  }

  private static class VariantSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT_SET;
    }

  }

  private static class CallSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALL_SET;
    }
  }

  private static class CallDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALL;
    }
  }


}
