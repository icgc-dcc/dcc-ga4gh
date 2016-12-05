package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.PortalFiles.getDataType;
import static org.collaboratory.ga4gh.loader.PortalFiles.getDonorId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getFileId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getObjectId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getSampleId;

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
    // val fileMetas = Portal.getFileMetasForNumDonors(2);
    val total = fileMetas.size();
    int counter = 1;
    for (val fileMeta : fileMetas) {
      log.info("Loading {}/{}", counter, total);
      try {
        loadFile(fileMeta);
      } catch (Exception e) {
        log.warn("Bad VCF with object id: {}", PortalFiles.getObjectId(fileMeta));
        log.warn("Message: {} ", e.getMessage());
      }
      counter++;
    }
  }

  private void loadFile(ObjectNode objectNode) {
    val objectId = getObjectId(objectNode);
    val fileId = getFileId(objectNode);
    val sampleId = getSampleId(objectNode);
    val donorId = getDonorId(objectNode);
    val dataType = getDataType(objectNode);
    val referenceName = PortalFiles.getReferenceName(objectNode);
    val genomeBuild = PortalFiles.getGenomeBuild(objectNode);
    val vcfFilenameParser = PortalFiles.getParser(objectNode);

    val fileMetaData =
        new FileMetaData(objectId, fileId, sampleId, donorId, dataType, referenceName, genomeBuild, vcfFilenameParser);

    log.info("Downloading file {}...", objectId);
    val file = Storage.downloadFile(objectId);

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, fileMetaData);
    val variants = vcf.readVariants();
    // val callSets = vcf.readCallSets();
    // val variantSets = vcf.readVariantSets();

    log.info("Indexing variants {}...", objectId);
    indexer.indexVariants(variants);

    /*
     * log.info("Indexing header {}...", sampleId); indexer.indexVariantSets(variantSets, sampleId);
     * 
     * log.info("Indexing callsets {}...", objectId); indexer.indexCallSets(callSets);
     */
  }

}
