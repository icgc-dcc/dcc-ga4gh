package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

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
  private static final int NUM_DONORS = 10;

  public void load() {
    indexer.prepareIndex();
    log.info("Resolving object ids...");

    val donorDataMap = Portal.getDonorDataMap(NUM_DONORS);
    int donorCount = 1;
    int donorTotal = donorDataMap.keySet().size();
    int globalFileMetaDataCount = 1;
    long globalFileMetaDataTotal = donorDataMap.values().stream()
        .map(x -> x.getTotalFileMetaCount())
        .collect(summingLong(Long::longValue));

    for (val donorDataEntry : donorDataMap.entrySet()) {
      val donorId = donorDataEntry.getKey();
      val donorData = donorDataEntry.getValue();
      donorData.dumpToJson(new File("target/rob_" + donorId + ".json"));
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
            String outputDir = "target/storedVCFs";
            downloadAndLoadFile(fileMetaData, outputDir);
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

  private void downloadAndLoadFile(@NonNull final FileMetaData fileMetaData) {
    log.info("Downloading file {}...", fileMetaData.getVcfFilenameParser().getFilename());
    File file = Storage.downloadFile(fileMetaData.getObjectId());
    loadFile(file, fileMetaData);
  }

  private void downloadAndLoadFile(@NonNull final FileMetaData fileMetaData, final String outputDir) {
    log.info("[PERSIST_MODE] Downloading file {} to {}...", fileMetaData.getVcfFilenameParser().getFilename(),
        outputDir);
    File file = Storage.downloadFileAndPersist(fileMetaData.getObjectId(),
        outputDir + File.separator + fileMetaData.getVcfFilenameParser().getFilename(),
        fileMetaData.getFileMd5sum());
    loadFile(file, fileMetaData);
  }

  private void loadFile(@NonNull final File file, @NonNull final FileMetaData fileMetaData) {

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
}
