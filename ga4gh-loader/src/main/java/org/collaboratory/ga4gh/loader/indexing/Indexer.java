package org.collaboratory.ga4gh.loader.indexing;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.loader.Config.INDEX_NAME;
import static org.collaboratory.ga4gh.loader.Config.MONITOR_INTERVAL_COUNT;
import static org.collaboratory.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.elasticsearch.common.xcontent.XContentType.SMILE;

import java.io.IOException;
import java.util.stream.Stream;

import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantCallPair;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.model.es.converters.EsCallConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantConverter;
import org.collaboratory.ga4gh.loader.model.es.converters.EsVariantSetConverter;
import org.collaboratory.ga4gh.loader.utils.CounterMonitor;
import org.collaboratory.ga4gh.loader.utils.cache.IdCache;
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
  private static final String INDEX_SETTINGS_JSON_FILENAME = "index.settings.json";
  public static final String CALLSET_TYPE_NAME = "callset";
  public static final String VARIANT_SET_TYPE_NAME = "variant_set";
  public static final String CALL_TYPE_NAME = "call";
  public static final String VARIANT_TYPE_NAME = "variant";
  public static final String VCF_HEADER_TYPE_NAME = "vcf_header";
  private static final ObjectWriter BINARY_WRITER = JacksonFactory.getObjectWriter();
  private static final String DEFAULT_MAPPINGS_DIRNAME = "org/collaboratory/ga4gh/resources/mappings";
  private static final String DEFAULT_MAPPING_JSON_EXTENSION = ".mapping.json";

  private static final EsVariantConverter VARIANT_CONVERTER = new EsVariantConverter();
  private static final EsVariantSetConverter VARIANT_SET_CONVERTER = new EsVariantSetConverter();
  private static final EsCallSetConverter CALL_SET_CONVERTER = new EsCallSetConverter();
  private static final EsCallConverter CALL_CONVERTER = new EsCallConverter();

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
  private final IdCache<EsVariant, Long> variantIdCache;

  @NonNull
  private final IdCache<String, Integer> variantSetIdCache;

  @NonNull
  private final IdCache<String, Integer> callSetIdCache;

  private int callId = 0;

  private final CounterMonitor variantMonitor = newMonitor("VariantIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor callMonitor = newMonitor("CallIndexing", MONITOR_INTERVAL_COUNT);
  private final CounterMonitor vcfHeaderMonitor = newMonitor("VCFHeaderIndexing", MONITOR_INTERVAL_COUNT);

  @SneakyThrows
  public void prepareIndex() {
    val indexerConfiguration = IndexConfiguration.builder()
        .client(client)
        .indexingEnabled(true)
        .indexName(indexName)
        .indexSettingsFilename(INDEX_SETTINGS_JSON_FILENAME)
        .mappingDirname(DEFAULT_MAPPINGS_DIRNAME)
        .mappingFilenameExtension(DEFAULT_MAPPING_JSON_EXTENSION)
        .typeName(CALLSET_TYPE_NAME)
        .typeName(VARIANT_SET_TYPE_NAME)
        .typeName(VARIANT_TYPE_NAME)
        .typeName(VCF_HEADER_TYPE_NAME)
        .typeName(CALL_TYPE_NAME)
        .build();
    val indexCreator = new IndexCreator(indexerConfiguration);
    indexCreator.execute();
  }

  private void writeVariantSet(final String variantSetId, @NonNull final EsVariantSet variantSet) throws IOException {
    writer.write(new IndexDocument(variantSetId, VARIANT_SET_CONVERTER.convertToObjectNode(variantSet),
        new VariantSetDocumentType()));
  }

  public IdCache<String, Integer> getVariantSetIdCache() {
    return variantSetIdCache;
  }

  public IdCache<String, Integer> getCallSetIdCache() {
    return callSetIdCache;
  }

  @SneakyThrows
  public void indexVariantSet(@NonNull final EsVariantSet variantSet) {
    val variantSetName = variantSet.getName();
    val isNewVariantSetId = !variantSetIdCache.contains(variantSetName);
    if (isNewVariantSetId) {
      variantSetIdCache.add(variantSetName);
      val variantSetId = variantSetIdCache.getIdAsString(variantSetName);
      writeVariantSet(variantSetId, variantSet);
    }
  }

  @SneakyThrows
  public void indexCallSet(@NonNull final EsCallSet callSet) {
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
    writer.write(new IndexDocument(callId, CALL_CONVERTER.convertToObjectNode(call), new CallDocumentType(),
        parentVariantId));

  }

  // TODO: [rtisma] rethink how will organize this data
  @SneakyThrows
  public void indexVCFHeader(final String objectId, @NonNull final ObjectNode vcfHeader) {
    val parent_variant_set_id = vcfHeader.path(VARIANT_SET_ID).textValue();
    checkState(
        client.prepareIndex(INDEX_NAME, VCF_HEADER_TYPE_NAME, objectId)
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
    writer.write(
        new IndexDocument(callSetId, CALL_SET_CONVERTER.convertToObjectNode(callSet), new CallSetDocumentType()));
  }

  private void writeVariant(final String variantId, @NonNull final EsVariant variant) throws IOException {
    writer
        .write(new IndexDocument(variantId, VARIANT_CONVERTER.convertToObjectNode(variant), new VariantDocumentType()));
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

}
