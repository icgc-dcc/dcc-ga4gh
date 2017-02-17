package org.collaboratory.ga4gh.loader;

import com.google.common.base.Stopwatch;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.collaboratory.ga4gh.core.model.converters.EsVariantConverter;
import org.collaboratory.ga4gh.loader.indexing.Indexer;
import org.collaboratory.ga4gh.loader.model.metadata.DonorData;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFetcher;
import org.collaboratory.ga4gh.loader.vcf.CallProcessorManager;
import org.collaboratory.ga4gh.loader.vcf.VCF;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Config.LOADER_MODE;
import static org.collaboratory.ga4gh.loader.Debug.dumpToJson;
import static org.collaboratory.ga4gh.loader.LoaderModes.NESTED_ONLY;
import static org.collaboratory.ga4gh.loader.LoaderModes.PARENT_CHILD_ONLY;
import static org.collaboratory.ga4gh.loader.LoaderModes.PARENT_CHILD_THEN_NESTED;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newClient;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newFileMetaDataFetcher;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newIdCacheFactory;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newLoader;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newNestedDocumentWriter;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newParentChild2NestedIndexConverter;
import static org.collaboratory.ga4gh.loader.factory.MainFactory.newParentChildDocumentWriter;
import static org.collaboratory.ga4gh.loader.model.metadata.DonorData.buildDonorDataList;
import static org.icgc.dcc.common.core.util.Formats.formatDuration;

@Slf4j
@RequiredArgsConstructor
public class Loader {

  @NonNull
  private final Indexer indexer;

  @NonNull
  private final Storage storage;

  @NonNull
  private final CallProcessorManager callProcessorManager;

  @NonNull
  private final EsVariantConverter variantConverter;

  private long globalFileMetaDataCount = -1;
  private long globalFileMetaDataTotal = -1;

  public static void main(String[] args) {
    log.info("Static Config:\n{}", Config.toConfigString());
    val idCacheFactory = newIdCacheFactory();
    try ( val pcClient = newClient();
        val nestedClient = newClient();
        //DefaultDocumentWriter.close() also closes the client, so need 2 clients (or more verbose trycatch).
        // Basically, when nesterWriter.close() is called, it calls client.close(),
        // but pcwriter might still be open and have active requests.
        val pcWriter = newParentChildDocumentWriter(pcClient);
        val nestedWriter = newNestedDocumentWriter(nestedClient)) {
      if (LOADER_MODE == PARENT_CHILD_ONLY || LOADER_MODE == PARENT_CHILD_THEN_NESTED) {
        idCacheFactory.build();
        val loader = newLoader(pcClient, pcWriter, idCacheFactory);
        val dataFetcher = newFileMetaDataFetcher();
        log.info("dataFetcher: {}", dataFetcher);

        log.info("Resolving object ids...");
        val fileMetaDataContext = dataFetcher.fetch();

        val watch = Stopwatch.createStarted();
        loader.loadUsingFileMetaDataContext(fileMetaDataContext);
        watch.stop();

        log.info("LoadTime: {}", formatDuration(watch));
      }
      if (LOADER_MODE == NESTED_ONLY || LOADER_MODE == PARENT_CHILD_THEN_NESTED) {
        val pc2nestedConverter = newParentChild2NestedIndexConverter(nestedClient, nestedWriter);
        pc2nestedConverter.execute();
      }
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

  public void loadUsingFileMetaDataContext(@NonNull final FileMetaDataContext fileMetaDataContext)
      throws ClassNotFoundException, IOException {
    indexer.prepareIndex();

    dumpToJson(fileMetaDataContext, "target/sorted_filemetaDatas.json");
    indexer.indexFileMetaDataContext(fileMetaDataContext);
    int count = 1;
    int total = fileMetaDataContext.size();
    for (val fileMetaData : fileMetaDataContext) {
      log.info("Loading FileMetaData {}/{}: {} -- {}", count, total, fileMetaData.getVcfFilenameParser().getFilename(),
          fileMetaData.getFileSizeMb());
      loadFileMetaData(fileMetaData);
      count++;
    }

    /*
    Perform any optimizations using the indexer
     */
    log.info("Starting parent child optimizations...");
    indexer.optimize();
    log.info("Finished parent child optimizations");
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
    val callProcessor = callProcessorManager.getCallProcessor(fileMetaData.getVcfFilenameParser().getCallerType());
    @Cleanup
    val vcf = new VCF(file,
        fileMetaData,
        indexer.getVariantSetIdCache(),
        indexer.getCallSetIdCache(),
        callProcessor,
        variantConverter);

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
