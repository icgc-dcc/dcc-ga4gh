package org.icgc.dcc.ga4gh.loader2;

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
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair2;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.Config;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreator;
import org.icgc.dcc.ga4gh.loader.indexing.IndexCreatorContext;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.context.IdStorageContext;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.IdStorage;

import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL;
import static org.icgc.dcc.ga4gh.common.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;

@Slf4j
@RequiredArgsConstructor
public class Indexer2 {

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
  @NonNull private final EsVariantCallPairConverterJson2 variantCallPairConverter;


  @NonFinal
  private IndexCreator indexCreator = null;


  private final CounterMonitor variantMonitor = newMonitor("VariantIndexing", Config.MONITOR_INTERVAL_COUNT);
  private final CounterMonitor vcfHeaderMonitor = newMonitor("VCFHeaderIndexing", Config.MONITOR_INTERVAL_COUNT);

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


  private <K, ID> void indexIdStorage(IdStorage<K,ID> idStorage, BiConsumer<K, ID> consumer){
    val iterator = idStorage.streamEntries().iterator();
    while(iterator.hasNext()){
      val entry = iterator.next();
      val k = entry.getKey();
      val id = entry.getValue();
      consumer.accept(k, id);
    }

  }

  @SneakyThrows
  public void indexVariants(@NonNull IdStorage<EsVariantCallPair2, IdStorageContext<Long, EsCall>> variantIdStorage){
    variantMonitor.start();
    indexIdStorage(variantIdStorage, this::writeVariant);
    variantMonitor.stop();
  }

  @SneakyThrows
  public void indexVariantSets(@NonNull IdStorage<EsVariantSet, Integer> variantSetIdStorage) {
    indexIdStorage(variantSetIdStorage, this::writeVariantSet);
  }

  @SneakyThrows
  public void indexCallSets(@NonNull IdStorage<EsCallSet, Integer> callSetIdStorage) {
    indexIdStorage(callSetIdStorage, this::writeCallSet);
  }

  @SneakyThrows
  private void writeVariant(@NonNull EsVariantCallPair2 esVariantCallPair, @NonNull IdStorageContext<Long, EsCall> idStorageContext){
    val variantId = idStorageContext.getId();
    val data = variantCallPairConverter.convertToObjectNode(esVariantCallPair);
    writer.write(new IndexDocument(variantId.toString(), data, new VariantDocumentType()));
    variantMonitor.incr();
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
