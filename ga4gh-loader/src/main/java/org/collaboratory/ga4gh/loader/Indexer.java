package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;

import org.elasticsearch.client.Client;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.IndexDocumentType;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;

import htsjdk.variant.vcf.VCFHeader;
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
  public static final String BIO_SAMPLE_TYPE_NAME = "bio_sample";
  public static final String VARIANT_TYPE_NAME = "variant";

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

  @SneakyThrows
  public void prepareIndex() {
    log.info("Preparing index {}...", indexName);
    val indexes = client.admin().indices();
    if (indexes.prepareExists(indexName).execute().get().isExists()) {
      checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
    }

    checkState(indexes.prepareCreate(indexName)
        .setSettings(read("index.settings.json").toString())
        .addMapping(CALLSET_TYPE_NAME, read("callset.mapping.json").toString())
        .addMapping(BIO_SAMPLE_TYPE_NAME, read("bio_sample.mapping.json").toString())
        .addMapping(VARIANT_TYPE_NAME, read("variant.mapping.json").toString())
        .execute().actionGet().isAcknowledged());
  }

  @SneakyThrows
  public void indexBioSamples(@NonNull ObjectNode bioSample, String objectId) {
    writer.write(new IndexDocument(objectId, bioSample, new BioSampleDocumentType()));
  }

  @SneakyThrows
  public void indexCallSets(@NonNull Iterable<ObjectNode> callSets) {

    for (val callSet : callSets) {
      writeCallSet(callSet);
    }
  }

  @SneakyThrows
  public void indexVariants(@NonNull Iterable<ObjectNode> variants) {
    for (val variant : variants) {
      writeVariant(variant);
    }
  }

  private void writeCallSet(ObjectNode callSet) throws IOException {
    writer.write(new IndexDocument(nextId(), callSet, new CallSetDocumentType()));
  }

  private void writeVariant(ObjectNode variant) throws IOException {
    writer.write(new IndexDocument(nextId(), variant, new VariantDocumentType()));
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

  private static class BioSampleDocumentType implements IndexDocumentType {

    @Override
    public String getIndexType() {
      return BIO_SAMPLE_TYPE_NAME;
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
