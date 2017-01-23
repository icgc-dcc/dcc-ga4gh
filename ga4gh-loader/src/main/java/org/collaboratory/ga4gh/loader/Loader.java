package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Debug.dumpToJson;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newFileMetaDataFetcher;
import static org.collaboratory.ga4gh.loader.Factory.newIdCacheFactory;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.model.metadata.DonorData.buildDonorDataList;

import java.io.File;
import java.util.List;

import org.collaboratory.ga4gh.loader.model.metadata.DonorData;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Loader {

  @NonNull
  private final Indexer indexer;

  @NonNull
  private final Storage storage;

  private long globalFileMetaDataCount = -1;
  private long globalFileMetaDataTotal = -1;

  public static void main(String[] args) {

    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient();
        val idCacheFactory = newIdCacheFactory().init();
        val writer = newDocumentWriter(client)) {
      val loader = newLoader(client, writer, idCacheFactory);
      val startMs = System.currentTimeMillis();
      // val dataFetcher = Factory.newFileMetaDataFetcherCustom();
      val dataFetcher = newFileMetaDataFetcher();
      log.info("dataFetcher: {}", dataFetcher);
      // loader.loadUsingDonorDatas(dataFetcher);
      loader.loadUsingFileMetaDatas(dataFetcher);
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
      log.info("\t\tLoading File({}): {}/{} | (Total {}/{}) -- {}", fileMetaData.getFileId(), fileMetaDataCount,
          fileMetaDataTotal,
          globalFileMetaDataCount,
          globalFileMetaDataTotal,
          fileMetaData.getFileSizeMb());
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
      downloadAndLoadFile(fileMetaData);
    } catch (Exception e) {
      log.warn("Bad VCF with fileMetaData: {}", fileMetaData);
      log.warn("Message [{}]: {} ", e.getClass().getName(), e.getMessage());
      log.warn("StackTrace: {} ", e);
    } finally {
      globalFileMetaDataCount++;
    }
  }

  public void loadUsingFileMetaDatas(@NonNull final FileMetaDataFetcher dataFetcher) {
    indexer.prepareIndex();
    log.info("Resolving object ids...");
    val fileMetaDatas = dataFetcher.fetch();
    Debug.dumpToJson(fileMetaDatas, "target/sorted_filemetaDatas.json");
    int count = 1;
    int total = fileMetaDatas.size();
    for (val fileMetaData : fileMetaDatas) {
      log.info("Loading FileMetaData {}/{}: {} -- {}", count, total, fileMetaData.getVcfFilenameParser().getFilename(),
          fileMetaData.getFileSizeMb());
      loadFileMetaData(fileMetaData);
      count++;
    }
  }

  public void loadUsingDonorDatas(@NonNull final FileMetaDataFetcher dataFetcher) {
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
    val file = storage.downloadFile(fileMetaData);
    log.info("Loading file {} ...", file.getAbsolutePath());
    loadFile(file, fileMetaData);
  }

  private void loadFile(@NonNull final File file, @NonNull final FileMetaData fileMetaData) {
    @Cleanup
    val vcf = new VCF(file, fileMetaData);

    log.info("\tReading variants ...");
    // val variants = vcf.readVariants();
    val variants = vcf.readVariantAndCalls();

    log.info("\tReading calls...");
    val callMap = vcf.streamCalls();

    log.info("\tReading call_sets ...");
    val callSets = vcf.readCallSets();

    log.info("\tReading variant_sets ...");
    val variantSet = vcf.readVariantSet();

    log.info("\tReading vcf_headers ...");
    val vcfHeader = vcf.readVCFHeader();

    log.info("\t\tIndexing variants and calls ...");
    indexer.indexVariantsAndCalls(variants);
    /*
     * log.info("\t\tIndexing variants ..."); indexer.indexVariants(variants);
     * 
     * log.info("\t\tIndexing calls ..."); indexer.indexCalls(callMap);
     */

    log.info("\t\tIndexing variantSets ...");
    indexer.indexVariantSet(variantSet);

    log.info("\t\tIndexing callsets ...");
    indexer.indexCallSet(callSets);

    log.info("\t\tIndexing vcfHeaders ...");
    indexer.indexVCFHeader(fileMetaData.getObjectId(), vcfHeader);
  }
}
