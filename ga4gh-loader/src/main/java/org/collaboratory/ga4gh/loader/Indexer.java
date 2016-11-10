package org.collaboratory.ga4gh.loader;

import static java.lang.ClassLoader.getSystemResourceAsStream;
import static org.elasticsearch.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.Base64;

import org.elasticsearch.client.Client;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.DocumentType;
import org.icgc.dcc.dcc.common.es.model.Document;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Indexer {

  private final Client client;
  private final DocumentWriter writer;
  private final String indexName;

  private int id = 1;

  @SneakyThrows
  public void prepareIndex() {

    log.info("Preparing index {}...", indexName);
    val indexes = client.admin().indices();
    if (indexes.prepareExists(indexName).execute().get().isExists()) {
      checkState(indexes.prepareDelete(indexName).execute().get().isAcknowledged());
    }

    val settings = new StringBuffer();
    val settingsReader =
        new BufferedReader(new InputStreamReader(getSystemResourceAsStream("mappings/index.settings.json")));
    settingsReader.lines().forEach(s -> settings.append(s));
    log.info(settings.toString());

    val mapping1 = new StringBuffer();
    val reader1 = new BufferedReader(new InputStreamReader(getSystemResourceAsStream("mappings/variant.json")));
    reader1.lines().forEach(s -> mapping1.append(s));

    val mapping2 = new StringBuffer();
    val reader2 = new BufferedReader(new InputStreamReader(getSystemResourceAsStream("mappings/callset.json")));
    reader2.lines().forEach(s -> mapping2.append(s));

    val retVal = indexes.prepareCreate(indexName)
        .setSettings(settings.toString())
        .addMapping(Config.TYPE_NAME, mapping1.toString()).addMapping("callset", mapping2.toString())
        .execute().actionGet().isAcknowledged();
    log.info("Index ret val is: {}", retVal);

  }

  @SneakyThrows
  public void indexHeaders(@NonNull VCFHeader header, String objectId) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(header);
    oos.close();
    val ser = Base64.getEncoder().encodeToString(baos.toByteArray());
    val obj = DEFAULT.createObjectNode();
    obj.put("vcf_header", ser);

    writer.write(new Document(objectId, obj, new HeaderDocumentType()));
  }

  @SneakyThrows
  public void indexVariants(@NonNull ObjectNode fileMeta, @NonNull Iterable<ObjectNode> variants) {
    for (val variant : variants) {
      writeVariant(variant);
    }
  }

  private void writeVariant(ObjectNode variant) throws IOException {
    writer.write(new Document(nextId(), variant, new VariantDocumentType()));
  }

  private String nextId() {
    return String.valueOf(id++);
  }

  private static class VariantDocumentType implements DocumentType {

    @Override
    public String getIndexType() {
      return Config.TYPE_NAME;
    }

  }

  private static class HeaderDocumentType implements DocumentType {

    @Override
    public String getIndexType() {
      return "callset";
    }

  }

}
