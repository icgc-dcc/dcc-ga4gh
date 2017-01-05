package org.collaboratory.ga4gh.loader;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingLong;
import static org.collaboratory.ga4gh.loader.Benchmarks.writeToNewFile;
import static org.collaboratory.ga4gh.loader.Debug.dumpToJson;
import static org.collaboratory.ga4gh.loader.DonorData.buildDonorDataList;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.collaboratory.ga4gh.loader.Factory.newLoader;
import static org.collaboratory.ga4gh.loader.FileMetaData.buildFileMetaDataList;
import static org.collaboratory.ga4gh.loader.FileMetaData.groupFileMetaDatasByMutationType;
import static org.collaboratory.ga4gh.loader.FileMetaData.groupFileMetaDatasBySubMutationType;
import static org.collaboratory.ga4gh.loader.Portal.getFileMetasForNumDonors;
import static org.collaboratory.ga4gh.loader.Storage.downloadFile;
import static org.collaboratory.ga4gh.loader.Storage.downloadFileAndPersist;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.io.File;
import java.util.List;

import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;

import com.google.common.collect.ImmutableList;

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

  /*
   * State
   */
  private long globalFileMetaDataCount = -1;
  private long globalFileMetaDataTotal = -1;

  @NonNull
  private final Indexer indexer;

  public static void main(String[] args) {
    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient(); val writer = newDocumentWriter(client)) {
      val loader = newLoader(client, writer);
      val startMs = System.currentTimeMillis();
      loader.load();
      val endMs = System.currentTimeMillis();
      val durationSec = (endMs - startMs) / 1000;
      val durationMin = (endMs - startMs) / (60000);
      log.info("LoadTime(min): {}", durationMin);
      log.info("LoadTime(sec): {}", durationSec);

    } catch (Exception e) {
      log.error("Exception running: ", e);
    }
  }

  private static List<FileMetaData> filterFileMetaDataBySize(@NonNull final Iterable<FileMetaData> fileMetaDatas,
      final long maxSizeBytes) {
    return stream(fileMetaDatas).filter(f -> f.getFileSize() <= maxSizeBytes).collect(toImmutableList());
  }

  /*
   * Filters by MutationType==somatic, SubMutationType==(snv_mnv or indel)
   */
  private static List<FileMetaData> filterFileMetaData(@NonNull final Iterable<FileMetaData> fileMetaDatas) {
    val listBuilder = ImmutableList.<FileMetaData> builder();
    val mutationGrouping = groupFileMetaDatasByMutationType(fileMetaDatas);
    if (mutationGrouping.containsKey(MutationTypes.somatic.toString())) {
      val somaticMutationsFileMetaDatas = mutationGrouping.get(MutationTypes.somatic.toString());
      val subMutationGrouping = groupFileMetaDatasBySubMutationType(somaticMutationsFileMetaDatas);
      boolean foundAtLeastOne = false;
      if (subMutationGrouping.containsKey(SubMutationTypes.indel.toString())) {
        foundAtLeastOne = true;
        listBuilder.addAll(subMutationGrouping.get(SubMutationTypes.indel.toString()));
      }
      if (subMutationGrouping.containsKey(SubMutationTypes.snv_mnv.toString())) {
        foundAtLeastOne = true;
        listBuilder.addAll(subMutationGrouping.get(SubMutationTypes.snv_mnv.toString()));
      }
      if (!foundAtLeastOne) {
        log.error("The input somatic filemetadatas does not have any indel or snv_mnv submutation types");
      }
    } else {
      log.error("The input filemetadatas does not have any somatic mutation types");
    }
    return listBuilder.build();
  }

  private void resetGlobalFileMetaDataCounters(@NonNull final List<DonorData> donorDataList) {
    globalFileMetaDataCount = 1;
    globalFileMetaDataTotal = donorDataList.stream()
        .map(x -> x.getTotalFileMetaCount())
        .collect(summingLong(Long::longValue));
  }

  private List<FileMetaData> fetchFileMetaData(final int numDonors) {
    val fileMetas = getFileMetasForNumDonors(numDonors);
    return buildFileMetaDataList(fileMetas);
  }

  private List<DonorData> fetchDonorData(final int numDonors, final long maxFileSizeBytes) {
    val fileMetaDatas = fetchFileMetaData(numDonors);
    writeToNewFile("./target/all_list.txt",
        stream(fileMetaDatas).map(x -> x.getVcfFilenameParser().getFilename())
            .collect(joining("\n")));

    // If size > 0, use only files less than or equal to maxFileSizeBytes
    val filteredFileMetaDatasBySize =
        (maxFileSizeBytes < 0) ? fileMetaDatas : filterFileMetaDataBySize(fileMetaDatas, maxFileSizeBytes);
    val filteredFileMetaDatas = filterFileMetaData(filteredFileMetaDatasBySize);
    writeToNewFile("./target/filtered_list.txt",
        stream(filteredFileMetaDatas).map(x -> x.getVcfFilenameParser().getFilename())
            .collect(joining("\n")));

    val donorDataList = buildDonorDataList(filteredFileMetaDatas);

    // Reset the counters for total global filemeta data objects
    resetGlobalFileMetaDataCounters(donorDataList);
    return donorDataList;
  }

  private List<DonorData> fetchDonorData(final int numDonors) {
    return fetchDonorData(numDonors, -1);
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
      String outputDir = "target/storedVCFs";
      downloadAndLoadFile(fileMetaData, outputDir);
      // downloadFileOnly(fileMetaData, outputDir);
    } catch (Exception e) {
      log.warn("Bad VCF with fileMetaData: {}", fileMetaData);
      log.warn("Message [{}]: {} ", e.getClass().getName(), e.getMessage());
      log.warn("StackTrace: {} ", e);
    } finally {
      globalFileMetaDataCount++;
    }
  }

  public void load() {
    indexer.prepareIndex();
    log.info("Resolving object ids...");
    val donorDataList = fetchDonorData(NUM_DONORS, DEBUG_FILEMETADATA_MAX_SIZE);
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
