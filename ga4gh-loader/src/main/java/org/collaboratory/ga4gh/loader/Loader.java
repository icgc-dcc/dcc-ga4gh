package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.FileMeta.getDonorId;
import static org.collaboratory.ga4gh.loader.FileMeta.getFileId;
import static org.collaboratory.ga4gh.loader.FileMeta.getObjectId;
import static org.collaboratory.ga4gh.loader.FileMeta.getSampleId;

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
    for (val fileMeta : fileMetas) {
      log.info("Loading {}/{}", counter, total);
      try {
        loadFile(fileMeta);
      } catch (Exception e) {
        log.warn("Bad VCF with object id: {}", FileMeta.getObjectId(fileMeta));
        log.warn("Message: {} ", e.getMessage());
      }
      counter++;
    }
  }

  private void loadFile(ObjectNode objectNode) {

    val objectId = getObjectId(objectNode);
    val fileId = getFileId(objectNode, Config.REPOSITORY_NAME);
    val sampleId = getSampleId(objectNode);
    val donorId = getDonorId(objectNode);

    val additionalSourceData = new FileMetaData(objectId, fileId, sampleId, donorId);

    log.info("Downloading file {}...", objectId);
    val file = Storage.downloadFile(objectId);

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, additionalSourceData);
    val variants = vcf.read();
    val header = vcf.getHeader();

    log.info("Indexing header {}...", objectId);
    indexer.indexHeaders(header, objectId);

    log.info("Indexing {}...", objectId);
    indexer.indexVariants(variants);
  }

}
