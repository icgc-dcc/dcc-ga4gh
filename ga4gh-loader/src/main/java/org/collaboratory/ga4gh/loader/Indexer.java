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
    }

    checkState(indexes.prepareCreate(indexName)
        .setSettings(read("index.settings.json").toString())
        .addMapping(CALLSET_TYPE_NAME, read(CALLSET_TYPE_NAME + ".mapping.json").toString())
        .addMapping(VARIANT_SET_TYPE_NAME, read(VARIANT_SET_TYPE_NAME + ".mapping.json").toString())
        .addMapping(VARIANT_TYPE_NAME, read(VARIANT_TYPE_NAME + ".mapping.json").toString())
        .addMapping(CALL_TYPE_NAME, read(CALL_TYPE_NAME + ".mapping.json").toString())
        .execute().actionGet().isAcknowledged());
  }

  @SneakyThrows
  public void indexVariantSet(@NonNull final ObjectNode variantSet) {
    val variantSetId = variantSet.path("id").textValue();
    if (this.variantSetIdCache.contains(variantSetId) == false) {
      writer.write(new IndexDocument(variantSetId, variantSet, new VariantSetDocumentType()));
      this.variantSetIdCache.add(variantSetId);
    }
  }

  @SneakyThrows
  public void indexCallSet(@NonNull final ObjectNode callSet) {
    val callSetId = callSet.path("id").textValue();
    if (this.callSetIdCache.contains(callSetId) == false) {
      writeCallSet(callSet);
      this.callSetIdCache.add(callSetId);
    }
  }

  @SneakyThrows
  public void indexCalls(Map<String, ObjectNode> variantId2CallMap) {
    for (val entry : variantId2CallMap.entrySet()) {
      val variantId = entry.getKey();
      val call = entry.getValue();
      writeCall(variantId, call);
    }
  }

  @SneakyThrows
  public void indexVariants(@NonNull Iterable<ObjectNode> variants) {
    for (val variant : variants) {
      writeVariant(variant);
    }
  }

  private static byte[] createSource(Object document) {
    try {
      return BINARY_WRITER.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw propagate(e);
    }
  }

  // TODO: [rtisma] make the caller do bulk calls
  @SneakyThrows
  private void writeCall(String parent_variant_id, ObjectNode call) {
    val callId = call.path("id").textValue();
    if (this.callIdCache.contains(callId) == false) {
      checkState(
          this.client.prepareIndex(Config.INDEX_NAME, CALL_TYPE_NAME, callId)
              .setContentType(SMILE)
              .setSource(createSource(call))
              .setParent(parent_variant_id)
              .setRouting(VARIANT_TYPE_NAME)
              .get().status().equals(RestStatus.CREATED));
      this.callIdCache.add(callId);
    }
  }

  private void writeCallSet(ObjectNode callSet) throws IOException {
    writer.write(new IndexDocument(callSet.path("id").textValue(), callSet, new CallSetDocumentType()));
  }

  // TODO: [rtisma] -- clean this up so not referencing "id" like this
  private void writeVariant(ObjectNode variant) throws IOException {
    val variant_id = variant.path("id").textValue();
    if (this.variantIdCache.contains(variant_id) == false) {
      writer.write(new IndexDocument(variant_id, variant, new VariantDocumentType()));
      this.variantIdCache.add(variant_id);
    }
  }

  private String nextId() {
    return String.valueOf(id++);
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
  private static ObjectNode read(String fileName) {
    val url = Resources.getResource(MAPPINGS_DIR + "/" + fileName);
    return (ObjectNode) DEFAULT.readTree(url);
  }

}
