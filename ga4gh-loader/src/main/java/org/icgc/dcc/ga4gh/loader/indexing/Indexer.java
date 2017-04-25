package org.icgc.dcc.ga4gh.loader.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import htsjdk.tribble.TribbleException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsCallConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsCallSetConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsVariantConverter;
import org.icgc.dcc.ga4gh.common.resources.model.converters.EsVariantSetConverter;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsCall;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsCallSet;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsVariant;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsVariantCallPair;
import org.icgc.dcc.ga4gh.common.resources.model.es.EsVariantSet;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaData;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.cache.id.IdCache;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.icgc.dcc.common.core.util.stream.Collectors;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.json.JacksonFactory;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;
import org.icgc.dcc.ga4gh.loader.Config;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.icgc.dcc.ga4gh.common.resources.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.CALL;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.CALL_SET;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VARIANT_SET;
import static org.icgc.dcc.ga4gh.common.resources.TypeNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;


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
  @NonNull
  private final IdCache<EsVariant, Long> variantIdCache;

  @NonNull
  private final IdCache<String, Integer> variantSetIdCache;

  @NonNull
  private final IdCache<String, Integer> callSetIdCache;

  @NonNull
  private final EsVariantSetConverter variantSetConverter;

  @NonNull
  private final EsCallSetConverter esCallSetConverter;

  @NonNull
  private final EsVariantConverter variantConverter;

  @NonNull
  private final EsCallConverter callConverter;

  private int callId = 0;

  @NonFinal
  private IndexCreator indexCreator = null;


  private final CounterMonitor variantMonitor = newMonitor("VariantIndexing", Config.MONITOR_INTERVAL_COUNT);
  private final CounterMonitor callMonitor = newMonitor("CallIndexing", Config.MONITOR_INTERVAL_COUNT);
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

  private void writeVariantSet(final String variantSetId, @NonNull final EsVariantSet variantSet) throws IOException {
    writer.write(new IndexDocument(variantSetId, variantSetConverter.convertToObjectNode(variantSet),
        new VariantSetDocumentType()));
  }

  public IdCache<String, Integer> getVariantSetIdCache() {
    return variantSetIdCache;
  }

  public IdCache<String, Integer> getCallSetIdCache() {
    return callSetIdCache;
  }

  public void indexFileMetaDataContext(@NonNull final FileMetaDataContext fileMetaDataContext) {
    log.info("Converting VariantSets from FileMetaDataContext...");
    val variantSets = convertToSetOfEsVariantSets(fileMetaDataContext);

    log.info("Indexing VariantSets ...");
    variantSets.forEach(this::indexVariantSet);

    log.info("Converting CallSets from FileMetaDataContext...");
    val callSets = convertToSetOfEsCallSets(fileMetaDataContext, variantSetIdCache);

    log.info("Indexing CallSets ...");
    callSets.forEach(this::indexCallSet);
  }

  private EsVariantSet convertToEsVariantSet(FileMetaData fileMetaData) {
    return EsVariantSet.builder()
        .name(fileMetaData.getVcfFilenameParser().getCallerId())
        .dataSetId(fileMetaData.getDataType())
        .referenceSetId(fileMetaData.getReferenceName())
        .build();
  }

  private Set<EsVariantSet> convertToSetOfEsVariantSets(FileMetaDataContext fileMetaDataContext) {
    return Streams.stream(fileMetaDataContext)
        .map(this::convertToEsVariantSet)
        .collect(Collectors.toImmutableSet());
  }

  private Set<EsCallSet> convertToSetOfEsCallSets(FileMetaDataContext fileMetaDataContext,
      IdCache<String, Integer> variantSetIdCache) {
    val groupedBySampleMap = fileMetaDataContext.groupFileMetaDataBySample();
    val setBuilder = ImmutableSet.<EsCallSet> builder();
    val converter = new EsVariantSetConverter();
    for (val entry : groupedBySampleMap.entrySet()) {
      val sampleName = entry.getKey();
      val fileMetaDataContextForSample = entry.getValue();
      val variantSetIds = convertToSetOfEsVariantSets(fileMetaDataContextForSample).stream()
          .map(vs -> variantSetIdCache.getId(vs.getName()))
          .collect(toImmutableSet());
      setBuilder.add(
          EsCallSet.builder()
              .name(sampleName)
              .variantSetIds(variantSetIds)
              .bioSampleId(sampleName) // bio_sample_id == call_set_name
              .build());
    }
    return setBuilder.build();
  }

  @SneakyThrows
  private void indexVariantSet(@NonNull final EsVariantSet variantSet) {
    val variantSetName = variantSet.getName();
    val isNewVariantSetId = !variantSetIdCache.contains(variantSetName);
    if (isNewVariantSetId) {
      variantSetIdCache.add(variantSetName);
      val variantSetId = variantSetIdCache.getIdAsString(variantSetName);
      writeVariantSet(variantSetId, variantSet);
    }
  }

  @SneakyThrows
  private void indexCallSet(@NonNull final EsCallSet callSet) {
    val callSetName = callSet.getName();
    val isNewCallSetId = !callSetIdCache.contains(callSetName);
    if (isNewCallSetId) {
      callSetIdCache.add(callSetName);
      val callSetId = callSetIdCache.getIdAsString(callSetName);
      writeCallSet(callSetId, callSet);
    }
  }

  @SneakyThrows
  private void processEsCall(final EsVariant parentVariant, final EsCall call) {
    val callName = call.getName();
    val doesVariantNameAlreadyExist = variantIdCache.contains(parentVariant);
    checkState(doesVariantNameAlreadyExist,
        "The variant Name: %s doesnt not exist for this call: %s. Make sure variant indexed BEFORE call index",
        parentVariant, callName);
    val parentVariantId = variantIdCache.getIdAsString(parentVariant);
    writeCall(parentVariantId, nextCallId(), call);
  }

  private String nextCallId() {
    return Integer.toString(++callId);
  }

  @SneakyThrows
  public void indexVariantsAndCalls(@NonNull final Stream<EsVariantCallPair> pair) {
    variantMonitor.start();
    callMonitor.start();
    try {
      pair.forEach(v -> indexSingleVariantAndCall(v));
    } catch (TribbleException te) {
      log.error("CORRUPTED VCF due to Variant -- Message [{}]: {}",
          te.getClass().getName(),
          te.getMessage());
    } finally {
      variantMonitor.stop();
      callMonitor.stop();

      variantMonitor.displaySummary();
      callMonitor.displaySummary();

      variantMonitor.reset();
      callMonitor.reset();
    }

  }

  @SneakyThrows
  private void indexSingleVariantAndCall(@NonNull final EsVariantCallPair pair) {
    val calls = pair.getCalls();
    val variant = pair.getVariant();
    val isNewVariantId = !variantIdCache.contains(variant);
    if (isNewVariantId) {
      variantIdCache.add(variant);
      val variantId = variantIdCache.getIdAsString(variant);
      writeVariant(variantId, variant);
      variantMonitor.incr();
    }
    for (val call : calls) {
      processEsCall(variant, call);
      callMonitor.incr();
    }
  }

  private static byte[] createSource(@NonNull final Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  private void writeCall(final String parentVariantId, final String callId, @NonNull final EsCall call)
      throws IOException {
    writer.write(new IndexDocument(callId, callConverter.convertToObjectNode(call), new CallDocumentType(),
        parentVariantId));

  }

  // TODO: [rtisma] rethink how will organize this data
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

  // Need builder so can finalize POJO with variantSetIds, which are only known after VariantSetIndexing is complete. So
  // doing it now
  private void writeCallSet(final String callSetId, @NonNull final EsCallSet callSet)
      throws IOException {
    writer.write(
        new IndexDocument(callSetId, esCallSetConverter.convertToObjectNode(callSet), new CallSetDocumentType()));
  }

  private void writeVariant(final String variantId, @NonNull final EsVariant variant) throws IOException {
    writer
        .write(new IndexDocument(variantId, variantConverter.convertToObjectNode(variant), new VariantDocumentType()));
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
