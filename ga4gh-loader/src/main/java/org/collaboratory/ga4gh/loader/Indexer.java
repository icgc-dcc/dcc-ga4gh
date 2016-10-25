package org.collaboratory.ga4gh.loader;

import static org.elasticsearch.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

import org.elasticsearch.client.Client;
import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.impl.DocumentType;
import org.icgc.dcc.dcc.common.es.model.Document;

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
    val mapping = new StringBuffer();
    Files.lines(Paths.get(Resources.getResource("mapping/variant.json").toURI())).forEach(s -> mapping.append(s));
    indexes.prepareCreate(indexName).addMapping(Config.TYPE_NAME, mapping.toString()).execute();
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
  public void indexVariants(@NonNull Iterable<ObjectNode> variants, String objectId) {
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
      return "header";
    }

  }

}
