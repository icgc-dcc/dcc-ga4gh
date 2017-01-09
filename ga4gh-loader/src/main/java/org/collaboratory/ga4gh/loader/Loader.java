package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Debug.dumpToJson;
import static org.collaboratory.ga4gh.loader.DonorData.buildDonorDataList;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.Storage.downloadFile;
import static org.collaboratory.ga4gh.loader.Storage.downloadFileAndPersist;

import java.io.File;
import java.util.List;

import org.collaboratory.ga4gh.loader.filemetadata.FileMetaData;
import org.collaboratory.ga4gh.loader.filemetadata.FileMetaDataFetcher;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Loader {

  private static final int NUM_DONORS = 30;
  private static final long DEBUG_FILEMETADATA_MAX_SIZE = 7000000;
  private static final String DOWNLOADED_VCFS_DIR = "target/storedVCFs";

  @NonNull
  private final Indexer indexer;

  private long globalFileMetaDataCount = -1;
  private long globalFileMetaDataTotal = -1;

  public static void main(String[] args) {
    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient(); val writer = newDocumentWriter(client)) {
      val loader = newLoader(client, writer);
      val startMs = System.currentTimeMillis();
      val dataFetcher = FileMetaDataFetcher.builder()
          .numDonors(NUM_DONORS)
          .maxFileSizeBytes(DEBUG_FILEMETADATA_MAX_SIZE)
          .somaticSSMsOnly(true)
          .build();
      loader.load(dataFetcher);
      val endMs = System.currentTimeMillis();
      val durationSec = (endMs - startMs) / 1000;
      val durationMin = (endMs - startMs) / (60000);
      log.info("LoadTime(min): {}", durationMin);
      log.info("LoadTime(sec): {}", durationSec);

    } catch (Exception e) {
      log.error("Exception running: ", e);
    }
  }

  private void resetGlobalFileMetaDataCounters(@NonNull final List<DonorData> donorDataList) {
    globalFileMetaDataCount = 1;
    globalFileMetaDataTotal = donorDataList.stream()
        .map(x -> x.getTotalFileMetaCount())
        .collect(summingLong(Long::longValue));
  }

  private void loadSample(@NonNull final List<FileMetaData> fileMetaDatas) {
    int fileMetaDataCount = 1;
    int fileMetaDataTotal = fileMetaDatas.size();
    for (val fileMetaData : fileMetaDatas) {
      log.info("\t\tLoading File({}): {}/{} | (Total {}/{})", fileMetaData.getFileId(), fileMetaDataCount,
          fileMetaDataTotal,
          globalFileMetaDataCount,
          globalFileMetaDataTotal);
      loadFileMetaData(fileMetaData);
      fileMetaDataCount++;
    }

  }

  private void loadDonor(@NonNull final DonorData donorData) {
    int sampleCount = 1;
    int sampleTotal = donorData.getNumSamples();
    for (val sampleEntry : donorData.getSampleDataListMap().entrySet()) {
      val sampleId = sampleEntry.getKey();
      log.info("\tLoading Sample({}): {}/{}", sampleId, sampleCount, sampleTotal);
      val sampleFileMetaDatas = sampleEntry.getValue();
      loadSample(sampleFileMetaDatas);
      sampleCount++;
    }
  }

  private void loadFileMetaData(@NonNull final FileMetaData fileMetaData) {
    try {
      downloadAndLoadFile(fileMetaData, DOWNLOADED_VCFS_DIR);
    } catch (Exception e) {
      log.warn("Bad VCF with fileMetaData: {}", fileMetaData);
      log.warn("Message [{}]: {} ", e.getClass().getName(), e.getMessage());
      log.warn("StackTrace: {} ", e);
    } finally {
      globalFileMetaDataCount++;
    }
  }

  public void load(@NonNull final FileMetaDataFetcher dataFetcher) {
    indexer.prepareIndex();
    log.info("Resolving object ids...");
    val donorDataList = buildDonorDataList(dataFetcher.fetch());
    resetGlobalFileMetaDataCounters(donorDataList);
    dumpToJson(donorDataList, "target/donorDataList.json");
    int donorCount = 1;
    int donorTotal = donorDataList.size();
    for (val donorData : donorDataList) {
      val donorId = donorData.getId();
      log.info("Loading Donor({}): {}/{}", donorId, donorCount, donorTotal);
      loadDonor(donorData);
      donorCount++;
    }
  }

  private void downloadAndLoadFile(@NonNull final FileMetaData fileMetaData) {
    log.info("Downloading file {}...", fileMetaData.getVcfFilenameParser().getFilename());
    File file = downloadFile(fileMetaData.getObjectId());
    loadFile(file, fileMetaData);
  }

  private static File downloadFileOnly(@NonNull final FileMetaData fileMetaData, final String outputDir) {
    log.info("[PERSIST_MODE] Downloading file {} to {}...", fileMetaData.getVcfFilenameParser().getFilename(),
        outputDir);
    return downloadFileAndPersist(fileMetaData.getObjectId(),
        outputDir + File.separator + fileMetaData.getVcfFilenameParser().getFilename(),
        fileMetaData.getFileMd5sum());
  }

  private void downloadAndLoadFile(@NonNull final FileMetaData fileMetaData, final String outputDir) {
    val file = downloadFileOnly(fileMetaData, outputDir);
    log.info("Loading file {} to {}...", fileMetaData.getVcfFilenameParser().getFilename(), outputDir);
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
