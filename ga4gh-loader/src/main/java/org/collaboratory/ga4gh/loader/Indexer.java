package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  private final Client client;
  private final DocumentWriter writer;

  /**
   * Configuration.
   */
  private final String indexName;

  /**
   * State.
   */
  private int id = 1;

  private final Set<String> variantIdCache = new HashSet<String>();
  private final Set<String> variantSetIdCache = new HashSet<String>();
  private final Set<String> callSetIdCache = new HashSet<String>();
  private final Set<String> callIdCache = new HashSet<String>();

  @SneakyThrows
  public void prepareIndex() {
    log.info("Preparing index {}...", indexName);
    val indexes = client.admin().indices();
    if (indexes.prepareExists(indexName).execute().get().isExists()) {
      checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
      log.info("Deleted existing [{}] index", indexName);
    }

    checkState(indexes.prepareCreate(indexName)
        .setSettings(read("index.settings.json").toString())
        .addMapping(CALLSET_TYPE_NAME, read(CALLSET_TYPE_NAME + ".mapping.json").toString())
        .addMapping(VARIANT_SET_TYPE_NAME, read(VARIANT_SET_TYPE_NAME + ".mapping.json").toString())
        .addMapping(VARIANT_TYPE_NAME, read(VARIANT_TYPE_NAME + ".mapping.json").toString())
        .addMapping(VCF_HEADER_TYPE_NAME, read(VCF_HEADER_TYPE_NAME + ".mapping.json").toString())
        .addMapping(CALL_TYPE_NAME, read(CALL_TYPE_NAME + ".mapping.json").toString())
        .execute().actionGet().isAcknowledged());
    log.info("Created new index [{}]", indexName);
  }

  @SneakyThrows
  public void indexVariantSet(@NonNull final ObjectNode variantSet) {
    val variantSetId = variantSet.path("id").textValue();
    if (variantSetIdCache.contains(variantSetId) == false) {
      writer.write(new IndexDocument(variantSetId, variantSet, new VariantSetDocumentType()));
      variantSetIdCache.add(variantSetId);
    }
  }

  @SneakyThrows
  public void indexCallSet(@NonNull final ObjectNode callSet) {
    val callSetId = callSet.path("id").textValue();
    if (callSetIdCache.contains(callSetId) == false) {
      writeCallSet(callSet);
      callSetIdCache.add(callSetId);
    }
  }

  @SneakyThrows
  public void indexCalls(@NonNull final Map<String, ObjectNode> variantId2CallMap) {
    for (val entry : variantId2CallMap.entrySet()) {
      val variantId = entry.getKey().toString();
      val call = entry.getValue();
      String callId = call.path("id").textValue();
      checkState(variantIdCache.contains(variantId),
          "The variant Id: %s doesnt not exist for this call: %s. Make sure variantId indexed BEFORE call index",
          variantId, callId);
      writeCall(variantId, call);
    }
  }

  @SneakyThrows
  public void indexVariants(@NonNull final Iterable<ObjectNode> variants) {
    for (val variant : variants) {
      writeVariant(variant);
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
  private void writeCall(final String parent_variant_id, @NonNull final ObjectNode call) {
    val callId = call.path("id").textValue();
    if (callIdCache.contains(callId) == false) {
      checkState(
          client.prepareIndex(Config.INDEX_NAME, CALL_TYPE_NAME, callId)
              .setContentType(SMILE)
              .setSource(createSource(call))
              .setParent(parent_variant_id)
              .get().status().equals(RestStatus.CREATED));
      callIdCache.add(callId);
    }
  }

  // TODO: [rtisma] make the caller do bulk calls
  // TODO: [rtisma] rethink how will organize this data
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    val parent_variant_set_id = vcfHeader.path("variant_set_id").textValue();
    checkState(
        client.prepareIndex(Config.INDEX_NAME, VCF_HEADER_TYPE_NAME, objectId)
            .setContentType(SMILE)
            .setSource(createSource(vcfHeader))
            .setParent(parent_variant_set_id)
            .setRouting(VARIANT_SET_TYPE_NAME)
            .get().status().equals(RestStatus.CREATED));
  }

  private void writeCallSet(@NonNull final ObjectNode callSet) throws IOException {
    val call_set_id = callSet.path("id").textValue();
    if (callSetIdCache.contains(call_set_id) == false) {
      writer.write(new IndexDocument(call_set_id, callSet, new CallSetDocumentType()));
      callSetIdCache.add(call_set_id);
    }
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

  @SneakyThrows
  private static ObjectNode read(final String fileName) {
    val url = Resources.getResource(MAPPINGS_DIR + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }
}
