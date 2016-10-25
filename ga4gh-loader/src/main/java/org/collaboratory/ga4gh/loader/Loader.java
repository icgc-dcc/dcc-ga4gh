package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Loader {

  public static void main(String[] args) {
    try (val client = newClient(); val writer = newDocumentWriter()) {
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
    val objectIds = Portal.getObjectIds();
    val total = objectIds.size();
    int counter = 1;
    for (val objectId : objectIds) {
      log.info("Loading {}/{}", counter, total);
      loadObject(objectId);
      counter++;
    }
  }

  private void loadObject(String objectId) {
    log.info("Downloading file {}...", objectId);
    val file = Storage.downloadFile(objectId);

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, objectId);
    val variants = vcf.read();
    val header = vcf.getHeader();

    log.info("Indexing header {}...", objectId);
    indexer.indexHeaders(header, objectId);

    log.info("Indexing {}...", objectId);
    indexer.indexVariants(variants, objectId);
  }

}
