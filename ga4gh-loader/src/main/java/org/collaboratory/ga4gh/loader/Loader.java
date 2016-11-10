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
    log.info("Static Config:\n" + Config.toConfigString());
    try {
      int numSeconds = 7;
      for (int i = 0; i < numSeconds; i++) {
        System.out.println((numSeconds - i) + " seconds left...");
        Thread.sleep(1000);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
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
    val fileMetas = Portal.getFileMetas();
    val total = fileMetas.size();
    int counter = 1;
    String objectId;
    for (val fileMeta : fileMetas) {
      log.info("Loading {}/{}", counter, total);
      objectId = fileMeta.get("objectId").textValue();
      try {
        loadObject(objectId);
      } catch (Exception e) {
        log.warn("Bad VCF with object id: {}", objectId);
      }
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
    indexer.indexVariants(variants);
  }

}
