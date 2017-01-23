package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.io.Resources.getResource;
import static org.collaboratory.ga4gh.loader.Config.MONITOR_INTERVAL_COUNT;
import static org.collaboratory.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_ID;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.utils.Countable;
import org.collaboratory.ga4gh.loader.utils.CounterMonitor;
import org.collaboratory.ga4gh.loader.utils.IdCache;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.json.JacksonFactory;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.tribble.TribbleException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Indexer {

  /**
   * Constants.
   */
  private static final String MAPPING_JSON_EXTENTION = ".mapping.json";
  private static final String INDEX_SETTINGS_JSON_FILENAME = "index.settings.json";
  public static final String CALLSET_TYPE_NAME = "callset";
  public static final String VARIANT_SET_TYPE_NAME = "variant_set";
  public static final String CALL_TYPE_NAME = "call";
  public static final String VARIANT_TYPE_NAME = "variant";
  public static final String VCF_HEADER_TYPE_NAME = "vcf_header";
  private static final ObjectWriter BINARY_WRITER = JacksonFactory.getObjectWriter();
  private static final String MAPPINGS_DIR = "org/collaboratory/ga4gh/resources/mappings";

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
  private final String indexName;

  /*
   * State
   */
  // Keys are strings NAMES, since those should never collide
  @NonNull
  private final IdCache<String> variantIdCache;
  @NonNull
  private final IdCache<String> variantSetIdCache;
  @NonNull
  private final IdCache<String> callSetIdCache;
  @NonNull
  private final IdCache<String> callIdCache;

  private final CounterMonitor callMonitor = newMonitor("CallIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor variantMonitor = newMonitor("VariantIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor variantSetMonitor = newMonitor("VariantSetIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor callSetMonitor = newMonitor("CallSetIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor vcfHeaderMonitor = newMonitor("VCFHeaderIndexing", MONITOR_INTERVAL_COUNT);

  @SneakyThrows
  public void prepareIndex() {
    log.info("Preparing index {}...", indexName);
    val indexes = client.admin().indices();
    if (indexes.prepareExists(indexName).execute().get().isExists()) {
      checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
      log.info("Deleted existing [{}] index", indexName);
    }

    val createIndexRequestBuilder = indexes.prepareCreate(indexName)
        .setSettings(read(INDEX_SETTINGS_JSON_FILENAME).toString());
    addMapping(createIndexRequestBuilder, CALLSET_TYPE_NAME);
    addMapping(createIndexRequestBuilder, VARIANT_SET_TYPE_NAME);
    addMapping(createIndexRequestBuilder, VARIANT_TYPE_NAME);
    addMapping(createIndexRequestBuilder, VCF_HEADER_TYPE_NAME);
    addMapping(createIndexRequestBuilder, CALL_TYPE_NAME);
    checkState(createIndexRequestBuilder.execute().actionGet().isAcknowledged());
    log.info("Created new index [{}]", indexName);
  }

  private static void addMapping(@NonNull final CreateIndexRequestBuilder builder, @NonNull final String typeName) {
    builder.addMapping(typeName, read(typeName + MAPPING_JSON_EXTENTION).toString());
  }

  private void writeVariantSet(final String variantSetId, @NonNull final EsVariantSet variantSet) throws IOException {
    writer.write(new IndexDocument(variantSetId, variantSet.toDocument(), new VariantSetDocumentType()));
  }

  @SneakyThrows
  public void indexVariantSet(@NonNull final EsVariantSet variantSet) {
    variantSetMonitor.start();
    val variantSetName = variantSet.getName();
    val isNewVariantSetId = !variantSetIdCache.contains(variantSetName);
    if (isNewVariantSetId) {
      variantSetIdCache.add(variantSetName);
      val variantSetId = variantSetIdCache.getIdAsString(variantSetName);
      writeVariantSet(variantSetId, variantSet);
      variantSetMonitor.incr();
    }
    variantSetMonitor.stop();
    log.info("[SUMMARY]: {}", variantSetMonitor);
    variantSetMonitor.reset();
  }

  @SneakyThrows
  public void indexCallSet(@NonNull final EsCallSet callSet) {
    callSetMonitor.start();
    val callSetName = callSet.getName();
    val isNewCallSetId = !callSetIdCache.contains(callSetName);
    if (isNewCallSetId) {
      callSetIdCache.add(callSetName);
      val callSetId = callSetIdCache.getIdAsString(callSetName);
      writeCallSet(callSetId, callSet);
      callSetMonitor.incr();
    }
    callSetMonitor.stop();
    log.info("[SUMMARY]: {}", callSetMonitor);
    callSetMonitor.reset();
  }

  private void processEsCall(final EsCall call, final Countable<Integer> counter) {
    val parentVariantName = call.getParentVariant().getName();
    val callName = call.getName();
    val doesVariantNameAlreadyExist = variantIdCache.contains(parentVariantName);
    checkState(doesVariantNameAlreadyExist,
        "The variant Name: %s doesnt not exist for this call: %s. Make sure variantName indexed BEFORE call index",
        parentVariantName, callName);
    val parentVariantId = variantIdCache.getIdAsString(parentVariantName);
    writeCall(parentVariantId, call);
    counter.incr();
  }

  @SneakyThrows
  public void indexCalls(final Stream<EsCall> stream) {
    callMonitor.start();
    try {
      stream.forEach(c -> processEsCall(c, callMonitor));
    } catch (TribbleException te) {
      log.error("CORRUPTED VCF due to Call -- Message [{}]: {}",
          te.getClass().getName(),
          te.getMessage());
    } finally {
      callMonitor.stop();
      log.info("[SUMMARY]: {}", callMonitor);
      callMonitor.reset();
    }
  }

  @SneakyThrows
  public void indexCallsOLD(final Map<String, EsCall> variantName2CallMap) {
    callMonitor.start();
    for (val entry : variantName2CallMap.entrySet()) {
      val parentVariantName = entry.getKey().toString();
      val call = entry.getValue();
      String callName = call.getName();
      val doesVariantNameAlreadyExist = variantIdCache.contains(parentVariantName);
      checkState(doesVariantNameAlreadyExist,
          "The variant Name: %s doesnt not exist for this call: %s. Make sure variantName indexed BEFORE call index",
          parentVariantName, callName);
      val parentVariantId = variantIdCache.getIdAsString(parentVariantName);
      writeCall(parentVariantId, call);
      callMonitor.incr();
    }
    callMonitor.stop();
    log.info("[SUMMARY]: {}", callMonitor);
    callMonitor.reset();
  }

  @SneakyThrows
  public void indexVariants(@NonNull final Iterable<EsVariant> variants) {
    variantMonitor.start();
    try {
      for (val variant : variants) {
        val variantName = variant.getName();
        val isNewVariantId = !variantIdCache.contains(variantName);
        if (isNewVariantId) {
          variantIdCache.add(variantName);
          val variantId = variantIdCache.getIdAsString(variantName);
          writeVariant(variantId, variant);
          variantMonitor.incr();
        }
      }
    } catch (TribbleException te) {
      log.error("CORRUPTED VCF due to Variant -- Message [{}]: {}",
          te.getClass().getName(),
          te.getMessage());
    } finally {
      variantMonitor.stop();
      log.info("[SUMMARY]: {}", variantMonitor);
      variantMonitor.reset();
      // NOTE: variant is not purged since the scope of variant ids is ALL files, where as
      // scope of call ids, callSet ids and variantSet ids are limited to currently processing file
    }
  }

  private static byte[] createSource(@NonNull final Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  // TODO: [rtisma] make the caller do bulk calls
  @SneakyThrows
  private void writeCall(final String parentVariantId, @NonNull final EsCall call) {
    val callName = call.getName();
    val isNewCallId = !callIdCache.contains(callName);
    if (isNewCallId) {
      callIdCache.add(callName);
      val callId = callIdCache.getIdAsString(callName);
      writer.write(new IndexDocument(callId, call.toDocument(), new CallDocumentType(),
          parentVariantId));
    }
  }

  // TODO: [rtisma] rethink how will organize this data
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    val parent_variant_set_id = vcfHeader.path(VARIANT_SET_ID).textValue();
    checkState(
        client.prepareIndex(Config.INDEX_NAME, VCF_HEADER_TYPE_NAME, objectId)
            .setContentType(SMILE)
            .setSource(createSource(vcfHeader))
            .setParent(parent_variant_set_id)
            .setRouting(VARIANT_SET_TYPE_NAME)
            .get().status().equals(RestStatus.CREATED));
  }

  // Need builder so can finalize POJO with variantSetIds, which are only known after VariantSetIndexing is complete. So
  // doing it now
  private void writeCallSet(final String callSetId, @NonNull final EsCallSet callSet)
      throws IOException {
    writer.write(new IndexDocument(callSetId, callSet.toDocument(), new CallSetDocumentType()));
  }

  private void writeVariant(final String variantId, @NonNull final EsVariant variant) throws IOException {
    writer.write(new IndexDocument(variantId, variant.toDocument(), new VariantDocumentType()));
  }

  private static class VariantDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return Indexer.VARIANT_TYPE_NAME;
    }
  }

  private static class VariantSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return VARIANT_SET_TYPE_NAME;
    }

  }

  private static class CallSetDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALLSET_TYPE_NAME;
    }
  }

  private static class CallDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return CALL_TYPE_NAME;
    }
  }

  @SneakyThrows
  private static ObjectNode read(final String fileName) {
    val url = getResource(MAPPINGS_DIR + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }

}
