package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Loader {

  public static void main(String[] args) {
    log.info("Static Config:\n" + Config.toConfigString());
    try (val client = newClient(); val writer = newDocumentWriter(client)) {
      val loader = newLoader(client, writer);
      loader.load();
    } catch (Exception e) {
      log.error("Exception running: ", e);
    }
  }

  @NonNull
  private final Indexer indexer;

  public void load() {
    indexer.prepareIndex();

    log.info("Resolving object ids...");
    val fileMetas = Portal.getFileMetas();
    val total = fileMetas.size();
    int counter = 1;
    String objectId;
    for (val fileMeta : fileMetas) {

      log.info("Loading {}/{}", counter, total);
      try {
        loadObjectNode(fileMeta);
      } catch (Exception e) {
        log.warn("Message: " + e.getMessage());
        e.printStackTrace();
        log.warn("Bad VCF with object id: {}", FileMetaUtils.getObjectId(fileMeta));
      }
      counter++;
    }
  }

  private void loadObjectNode(ObjectNode objectNode) {

    val objectId = FileMetaUtils.getObjectId(objectNode);
    val fileId = FileMetaUtils.getFileId(objectNode, Config.REPOSITORY_NAME);
    val sampleId = FileMetaUtils.getSampleId(objectNode);
    val donorId = FileMetaUtils.getDonorId(objectNode);

    val additionalIndicesData = AdditionalIndicesData.builder()
        .objectId(objectId)
        .fileId(fileId)
        .sampleId(sampleId)
        .donorId(donorId)
        .build();

    log.info("Downloading file {}...", objectId);
    val file = Storage.downloadFile(objectId);

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, additionalIndicesData);
    val variants = vcf.read();
    val header = vcf.getHeader();

    log.info("Indexing header {}...", objectId);
    indexer.indexHeaders(header, objectId);

    log.info("Indexing {}...", objectId);
    indexer.indexVariants(variants);
  }

}
