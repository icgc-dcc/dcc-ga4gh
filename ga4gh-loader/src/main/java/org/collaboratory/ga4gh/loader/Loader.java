package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Debug.dumpToJson;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newFileMetaDataFetcher;
import static org.collaboratory.ga4gh.loader.Factory.newIdCacheFactory;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.VCF.readVariantSet;
import static org.collaboratory.ga4gh.loader.model.metadata.DonorData.buildDonorDataList;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.collaboratory.ga4gh.loader.indexing.Indexer;
import org.collaboratory.ga4gh.loader.model.contexts.FileMetaDataContext;
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
    val idCacheFactory = newIdCacheFactory();
    try (val client = newClient();
        val writer = newDocumentWriter(client)) {
      idCacheFactory.build();
      val loader = newLoader(client, writer, idCacheFactory);
      val startMs = System.currentTimeMillis();
      val dataFetcher = newFileMetaDataFetcher();
      log.info("dataFetcher: {}", dataFetcher);
      loader.loadUsingFileMetaDataContext(dataFetcher);
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

  private void loadSample(@NonNull final FileMetaDataContext fileMetaDataContext) {
    int fileMetaDataCount = 1;
    int fileMetaDataTotal = fileMetaDataContext.size();
    for (val fileMetaData : fileMetaDataContext) {
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
      val sampleFileMetaDataContext = sampleEntry.getValue();
      loadSample(sampleFileMetaDataContext);
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

  public void loadUsingFileMetaDataContext(@NonNull final FileMetaDataFetcher dataFetcher)
      throws ClassNotFoundException, IOException {
    indexer.prepareIndex();
    log.info("Resolving object ids...");
    val fileMetaDataContext = dataFetcher.fetch();
    // val fileMetaDatas = FileMetaData.filter(dataFetcher.fetch(),
    // x -> !x.getVcfFilenameParser().getCallerType().getRealName().contains("broad")
    // && x.getVcfFilenameParser().getCallerType() != CallerTypes.consensus);

    dumpToJson(fileMetaDataContext, "target/sorted_filemetaDatas.json");
    loadVariantSetAndCallSets(fileMetaDataContext);
    int count = 1;
    int total = fileMetaDataContext.size();
    for (val fileMetaData : fileMetaDataContext) {
      log.info("Loading FileMetaData {}/{}: {} -- {}", count, total, fileMetaData.getVcfFilenameParser().getFilename(),
          fileMetaData.getFileSizeMb());
      loadFileMetaData(fileMetaData);
      count++;
    }
  }

  /*
   * TODO: very dirty. need to refactor. way to coupled
   */
  private void loadVariantSetAndCallSets(final FileMetaDataContext fileMetaDataContext) {
    log.info("\tLoading variant_sets ...");
    for (val fileMetaData : fileMetaDataContext) {
      val variantSet = readVariantSet(fileMetaData);
      indexer.indexVariantSet(variantSet);
    }
    log.info("\tLoading callsets ...");
    val sampleMap = FileMetaData.groupFileMetaDatasBySamplesThenCallers(fileMetaDataContext);
    val variantSetIdCache = indexer.getVariantSetIdCache();
    for (val fileMetaData : fileMetaDataContext) {
      val callSet = VCF.readCallSet(sampleMap, variantSetIdCache, fileMetaData);
      indexer.indexCallSet(callSet);
    }
  }

  public void loadUsingDonorDatas(@NonNull final FileMetaDataFetcher dataFetcher)
      throws ClassNotFoundException, IOException {
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
    val vcf = new VCF(file, fileMetaData, indexer.getVariantSetIdCache(), indexer.getCallSetIdCache());

    log.info("\tReading variants ...");
    val variants = vcf.readVariantAndCalls();

    log.info("\t\tIndexing variants and calls ...");
    indexer.indexVariantsAndCalls(variants);

    /*
     * TODO: need to fix this to index properly log.info("\tReading vcf_headers ..."); val vcfHeader =
     * vcf.readVCFHeader();
     * 
     * log.info("\t\tIndexing vcfHeaders ..."); indexer.indexVCFHeader(fileMetaData.getObjectId(), vcfHeader);
     */
  }
}
