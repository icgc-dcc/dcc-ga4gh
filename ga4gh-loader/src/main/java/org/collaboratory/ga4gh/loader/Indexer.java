package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.collaboratory.ga4gh.loader.model.es.IdCache.newIdCache;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_ID;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.IOException;
import java.util.Map;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.model.es.IdCache;
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
import com.google.common.io.Resources;

import htsjdk.samtools.util.StopWatch;
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
  private static final long INITIAL_ID = 1L;

  /**
   * Dependencies.
   */
  private final Client client;
  private final DocumentWriter writer;

  /**
   * Configuration.
   */
  private final String indexName;

  /*
   * State
   */
  // Keys are strings NAMES, since those should never collide
  private final IdCache<String> variantIdCache = IdCache.newIdCache(INITIAL_ID);
  private final IdCache<String> variantSetIdCache = newIdCache(INITIAL_ID);
  private final IdCache<String> callSetIdCache = newIdCache(INITIAL_ID);
  private final IdCache<String> callIdCache = newIdCache(INITIAL_ID);

  private final StopWatch watch = new StopWatch();

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
    writer.write(new IndexDocument(variantSetId, variantSet.toObjectNode(), new VariantSetDocumentType()));
  }

  @SneakyThrows
  public void indexVariantSet(@NonNull final EsVariantSet variantSet) {
    startWatch();
    val variantSetName = variantSet.getName();
    val isNewVariantSetId = variantSetIdCache.add(variantSetName);
    if (isNewVariantSetId) {
      val variantSetId = variantSetIdCache.getIdAsString(variantSetName);
      writeVariantSet(variantSetId, variantSet);
    }
    stopWatch();
    log.info("[StopWatch][indexVariantSet]: {} ms", durationWatch());
  }

  @SneakyThrows
  public void indexCallSet(@NonNull final EsCallSet callSet) {
    startWatch();
    val callSetName = callSet.getName();
    val isNewCallSetId = callSetIdCache.add(callSetName);
    if (isNewCallSetId) {
      val callSetId = callSetIdCache.getIdAsString(callSetName);
      writeCallSet(callSetId, callSet);
    }
    stopWatch();
    log.info("[StopWatch][indexCallSet]: {} ms", durationWatch());
  }

  private void startWatch() {
    watch.reset();
    watch.start();
  }

  private void stopWatch() {
    watch.stop();
  }

  private long durationWatch() {
    return watch.getElapsedTime();
  }

  @SneakyThrows
  public void indexCalls(final Map<String, EsCall> variantName2CallMap) {
    startWatch();
    for (val entry : variantName2CallMap.entrySet()) {
      val variantName = entry.getKey().toString();
      val call = entry.getValue();
      String callName = call.getName();
      val doesVariantNameAlreadyExist = variantIdCache.contains(variantName);
      checkState(doesVariantNameAlreadyExist,
          "The variant Id: %s doesnt not exist for this call: %s. Make sure variantId indexed BEFORE call index",
          variantName, callName);
      writeCall(variantName, call);
    }
    stopWatch();
    log.info("[StopWatch][indexCalls][{}]: {} ms", variantName2CallMap.values().size(), durationWatch());
  }

  @SneakyThrows
  public void indexVariants(@NonNull final Iterable<ObjectNode> variants) {
    startWatch();
    for (val variant : variants) {
      writeVariant(variant);
    }
    stopWatch();
    log.info("[StopWatch][indexVariants]: {} ms", durationWatch());
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
  private void writeCall(final String parent_variant_name, @NonNull final EsCall call) {
    val callName = call.getName();
    val isNewCallId = callIdCache.add(callName);
    if (isNewCallId) {
      writer.write(new IndexDocument(callIdCache.getIdAsString(callName), call.toObjectNode(), new CallDocumentType(),
          parent_variant_name));
    }
  }

  // TODO: [rtisma] rethink how will organize this data
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    startWatch();
    val parent_variant_set_id = vcfHeader.path(VARIANT_SET_ID).textValue();
    checkState(
        client.prepareIndex(Config.INDEX_NAME, VCF_HEADER_TYPE_NAME, objectId)
            .setContentType(SMILE)
            .setSource(createSource(vcfHeader))
            .setParent(parent_variant_set_id)
            .setRouting(VARIANT_SET_TYPE_NAME)
            .get().status().equals(RestStatus.CREATED));
    stopWatch();
    log.info("[StopWatch][indexVCFHeader]: {} ms", durationWatch());
  }

  private void writeCallSet(final String callSetId, @NonNull final EsCallSet callSet) throws IOException {
    writer.write(new IndexDocument(callSetId, callSet.toObjectNode(), new CallSetDocumentType()));
  }

  // TODO: [rtisma] -- clean this up so not referencing "id" like this
  private void writeVariant(@NonNull final ObjectNode variant) throws IOException {
    val variant_id = variant.path("id").textValue();
    if (variantIdCache.contains(variant_id) == false) {
      writer.write(new IndexDocument(variant_id, variant, new VariantDocumentType()));
      variantIdCache.add(variant_id);
    }
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
    val url = Resources.getResource(MAPPINGS_DIR + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }
}
