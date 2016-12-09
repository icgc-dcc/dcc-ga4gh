package org.collaboratory.ga4gh.loader;

import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.PortalFiles.getDataType;
import static org.collaboratory.ga4gh.loader.PortalFiles.getDonorId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getFileId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getObjectId;
import static org.collaboratory.ga4gh.loader.PortalFiles.getSampleId;

import java.util.Arrays;
import java.util.stream.Collectors;

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

    val donorDataMap = Portal.getDonorDataMap(9);
    int donorCount = 1;
    int donorTotal = donorDataMap.keySet().size();
    int globalFileMetaDataCount = 1;
    long globalFileMetaDataTotal = donorDataMap.values().stream()
        .map(x -> x.getTotalFileMetaCount())
        .collect(Collectors.summingLong(Long::longValue));

    for (val donorDataEntry : donorDataMap.entrySet()) {
      val donorId = donorDataEntry.getKey();
      val donorData = donorDataEntry.getValue();
      log.info("Loading Donor({}): {}/{}", donorId, donorCount, donorTotal);
      int sampleCount = 1;
      int sampleTotal = donorData.getNumSamples();
      for (val sampleEntry : donorData.getSampleDataListMap().entrySet()) {
        val sampleId = sampleEntry.getKey();
        val fileMetaDataList = sampleEntry.getValue();
        int fileMetaDataCount = 1;
        int fileMetaDataTotal = fileMetaDataList.size();
        log.info("\tLoading Sample({}): {}/{}", sampleId, sampleCount, sampleTotal);
        for (val fileMetaData : fileMetaDataList) {
          log.info("\t\tLoading File({}): {}/{} | (Total {}/{})", fileMetaData.getFileId(), fileMetaDataCount,
              fileMetaDataTotal,
              globalFileMetaDataCount,
              globalFileMetaDataTotal);
          try {
            loadFile(fileMetaData);
          } catch (Exception e) {
            log.warn("Bad VCF with fileMetaData: {}", fileMetaData);
            log.warn("Message: {} ", e.getMessage());
            log.warn("StackTrace: {} ",
                Arrays.stream(e.getStackTrace()).map(x -> x.toString()).collect(Collectors.joining("\n")));
          }
          fileMetaDataCount++;
          globalFileMetaDataCount++;
        }
        sampleCount++;
      }
      donorCount++;
    }

  }

  public void load2() {
    indexer.prepareIndex();
    log.info("Resolving object ids...");
    // val fileMetas = Portal.getFileMetas();
    // val donors = Portal.getDonorIds();

    val fileMetas = Portal.getFileMetasForNumDonors(3);
    DonorData.writeDonorDataMap("target/rob_fileMetasFor" + 20 + "donors.txt", fileMetas);

    val total = fileMetas.size();
    int counter = 1;
    for (val fileMeta : fileMetas) {
      log.info("Loading {}/{}", counter, total);
      try {
        loadFile(fileMeta);
      } catch (Exception e) {
        log.warn("Bad VCF with object id: {}", PortalFiles.getObjectId(fileMeta));
        log.warn("Message: {} ", e.getMessage());
        log.warn("StackTrace: {} ",
            Arrays.stream(e.getStackTrace()).map(x -> x.toString()).collect(Collectors.joining("\n")));
      }
      counter++;
    }
  }

  private void loadFile(@NonNull final FileMetaData fileMetaData) {
    log.info("Downloading file {}...", fileMetaData.getVcfFilenameParser().getFilename());
    val file = Storage.downloadFile(fileMetaData.getObjectId());

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, fileMetaData);
    val variants = vcf.readVariants();
    val callMap = vcf.readCalls();
    val callSets = vcf.readCallSets();
    val variantSet = vcf.readVariantSet();
    val vcfHeader = vcf.readVCFHeader();

    log.info("Indexing variants {}...", fileMetaData.getVcfFilenameParser().getFilename());
    indexer.indexVariants(variants);

    log.info("Indexing calls {}...", fileMetaData.getVcfFilenameParser().getFilename());
    indexer.indexCalls(callMap);

    log.info("Indexing callsets {}...", fileMetaData.getVcfFilenameParser().getFilename());
    indexer.indexCallSet(callSets);

    log.info("Indexing variantSets {}...", fileMetaData.getVcfFilenameParser().getFilename());
    indexer.indexVariantSet(variantSet);

    log.info("Indexing vcfHeaders {}...", fileMetaData.getVcfFilenameParser().getFilename());
    indexer.indexVCFHeader(fileMetaData.getObjectId(), vcfHeader);

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

    if (vcfFilenameParser.getMutationSubType().equals("indel")) {
      log.warn("Skipping file {} since its INDEL", vcfFilenameParser.getFilename());
      return;
    }

    val fileMetaData =
        new FileMetaData(objectId, fileId, sampleId, donorId, dataType, referenceName, genomeBuild, vcfFilenameParser);

    log.info("Downloading file {}...", vcfFilenameParser.getFilename());
    val file = Storage.downloadFile(objectId);

    log.info("Reading variants from {}...", file);
    @Cleanup
    val vcf = new VCF(file, fileMetaData);
    val variants = vcf.readVariants();
    val callMap = vcf.readCalls();
    val callSets = vcf.readCallSets();
    val variantSet = vcf.readVariantSet();
    val vcfHeader = vcf.readVCFHeader();

    log.info("Indexing variants {}...", objectId);
    indexer.indexVariants(variants);

    log.info("Indexing calls {}...", vcfFilenameParser.getFilename());
    indexer.indexCalls(callMap);

    log.info("Indexing callsets {}...", objectId);
    indexer.indexCallSet(callSets);

    log.info("Indexing variantSets {}...", sampleId);
    indexer.indexVariantSet(variantSet);

    log.info("Indexing vcfHeaders {}...", sampleId);
    indexer.indexVCFHeader(objectId, vcfHeader);

  }

}
