package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Config.PORTAL_API;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.util.set.Sets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class Portal {

  private static final int PORTAL_FETCH_SIZE = 100;
  private static final String REPOSITORY_NAME = "Collaboratory - Toronto";
  private static final String FILE_FORMAT = "VCF";
  private static final int DEFAULT_FILE_FROM = 1;
  private static final int DEFAULT_BUF_FILE_SIZE = 100;
  private static final int DEFAULT_BUF_DONOR_SIZE = 50;

  public static void main(String[] args) {
    try (val fileChannel = getFileChannel("target/robi2.txt")) {
      val fileMetas = getFileMetas();
      for (ObjectNode fileMeta : fileMetas) {
        String fileName = PortalFiles.getFileName(fileMeta);
        myWriterLn(fileChannel, fileName);

      }

    } catch (IOException e) {
      e.printStackTrace();

    }

  }

  public static FileChannel getFileChannel(String outputFn) throws IOException {
    Set<StandardOpenOption> options = Sets.newHashSet(
        StandardOpenOption.APPEND,
        StandardOpenOption.CREATE);
    return FileChannel.open(Paths.get(outputFn), options);

  }

  public static void myWriterLn(@NonNull final FileChannel fileChannel, String message) throws IOException {
    fileChannel.write(ByteBuffer.wrap((message + "\n").getBytes()));
  }

  public static void myWriter(@NonNull final FileChannel fileChannel, String message) throws IOException {
    fileChannel.write(ByteBuffer.wrap(message.getBytes()));
  }

  public static Map<String, DonorData> getDonorDataMap() {
    return getDonorDataMap(1, -1);
  }

  // numDonors == -1 means get all the donors possible
  public static Map<String, DonorData> getDonorDataMap(final int donorStart, final int numDonors) {
    int localDonorFrom = donorStart;
    val donorDataMap = new HashMap<String, DonorData>();
    int donorFrom = donorStart;
    while (true) {
      int buffDonorSize =
          (numDonors < 0) ? DEFAULT_BUF_DONOR_SIZE : Math.min(donorStart + numDonors - donorFrom,
              DEFAULT_BUF_DONOR_SIZE);
      val donors = getDonors(localDonorFrom, buffDonorSize);
      for (String donorId : donors) {
        System.out.println("Donor: " + donorId);
        val donorData = new DonorData(donorId);
        for (FileMetaData sampleFileMetaData : getSampleFileMetas(donorId)) {
          donorData.addSample(sampleFileMetaData.getSampleId(), sampleFileMetaData);
        }
        donorDataMap.put(donorId, donorData);
      }
      if (Iterables.size(donors) < buffDonorSize) {
        break;
      }
      localDonorFrom += buffDonorSize;
    }
    return donorDataMap;
  }

  public static List<FileMetaData> getSampleFileMetas(String donorId) {
    return getSampleFileMetas(donorId, 1, -1);
  }

  public static List<FileMetaData> getSampleFileMetas(String donorId, final int startFrom, final int numFiles) {
    val samplesFileMetaDataList = new ArrayList<FileMetaData>();
    int fileFrom = startFrom;
    while (true) {
      int buffFileSize =
          (numFiles < 0) ? DEFAULT_BUF_FILE_SIZE : Math.min(startFrom + numFiles - fileFrom, DEFAULT_BUF_FILE_SIZE);
      val url = getFilesForDonerUrl(donorId, fileFrom, buffFileSize);
      val result = read(url);
      val hits = getHits(result);
      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        val objectId = PortalFiles.getObjectId(fileMeta);
        val fileId = PortalFiles.getFileId(fileMeta);
        val sampleId = PortalFiles.getSampleId(fileMeta);
        val dataType = PortalFiles.getDataType(fileMeta);
        val referenceName = PortalFiles.getReferenceName(fileMeta);
        val genomeBuild = PortalFiles.getGenomeBuild(fileMeta);
        val vcfFilenameParser = PortalFiles.getParser(fileMeta);
        samplesFileMetaDataList.add(
            new FileMetaData(objectId, fileId, sampleId, donorId, dataType, referenceName, genomeBuild,
                vcfFilenameParser));
      }

      if (hits.size() < buffFileSize) {
        break;
      }
      fileFrom += buffFileSize;

    }
    return samplesFileMetaDataList;
  }

  public static List<DonorData> getDonorDataList() {
    val donorDataList = ImmutableList.<DonorData> builder();
    return donorDataList.build();
  }

  /**
   * Gets all Collaboratory VCF files.
   */
  public static List<ObjectNode> getFileMetas() {
    val fileMetas = ImmutableList.<ObjectNode> builder();
    val size = 100;// PORTAL_FETCH_SIZE;
    int from = 1;

    while (from < 21) {
      val url = getUrl(size, from);
      val result = read(url);
      val hits = getHits(result);

      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        fileMetas.add(fileMeta);
      }

      if (hits.size() < size) {
        break;
      }

      from += size;
    }

    return fileMetas.build();
  }

  public static List<ObjectNode> getFileMetasForNumDonors(int numDonors) {
    checkState(numDonors > 1); // due to bug in Portal api, must be greater than 1
    val fileMetas = ImmutableList.<ObjectNode> builder();

    int from = 1;
    val donorIterable = getDonors(from, numDonors);

    int size = PORTAL_FETCH_SIZE;
    while (true) {
      val allFilesForDonorsUrl = getFilesForDonersUrl(donorIterable, size, from);
      val result = read(allFilesForDonorsUrl);
      val hits = getHits(result);
      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        fileMetas.add(fileMeta);
      }
      if (hits.size() < size) {
        break;
      }
      from += size;
    }
    return fileMetas.build();
  }

  private static String getIdFromHit(JsonNode hit) {
    return hit.path("id").textValue();
  }

  // TODO: [rtisma] - donorIds retrieved from here are not searchable in repository. need to investigate why
  private static Iterable<String> getDonors(final int startPos, final int numDonors) {
    checkState(numDonors > 0);
    checkState(startPos > 0);
    val url = getDonersUrl(numDonors, startPos);
    val result = read(url);
    val hits = getHits(result);
    return transform(hits, Portal::getIdFromHit);

  }

  private static JsonNode getHits(JsonNode result) {
    return result.get("hits");
  }

  @SneakyThrows
  private static URL getDonersUrl(int size, int from) {
    val endpoint = PORTAL_API + "/api/v1/donors";
    return new URL(endpoint + "?" + "from=" + from + "&size=" + size + "&order=desc&facetsOnly=false");
  }

  @SneakyThrows
  private static URL getFilesForDonerUrl(final String donorId, final int from, final int size) {
    val endpoint = PORTAL_API + "/api/v1/repository/files";

    // {"file":{"repoName":{"is":["Collaboratory - Toronto"]},"fileFormat":{"is":["VCF"]},"donorId":{"is":["DO222843"]}}
    String filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + REPOSITORY_NAME + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + FILE_FORMAT + "\"]},"
        + "\"donorId\":{\"is\":[\"" + donorId + "\"]}}}", UTF_8.name());
    return new URL(
        endpoint + "?" + "filters=" + filters + "&" + "from=" + from + "&" + "size=" + size + "&sort=id&order=desc");
  }

  @SneakyThrows
  private static URL getFilesForDonersUrl(Iterable<String> donorIterable, int size, int from) {
    val endpoint = PORTAL_API + "/api/v1/repository/files";

    String donorsCSV = Lists.newArrayList(donorIterable).stream().collect(Collectors.joining("\",\""));
    // {"file":{"repoName":{"is":["Collaboratory - Toronto"]},"fileFormat":{"is":["VCF"]},"donorId":{"is":["DO222843"]}}
    String filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + REPOSITORY_NAME + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + FILE_FORMAT + "\"]},"
        + "\"donorId\":{\"is\":[\"" + donorsCSV + "\"]}}}", UTF_8.name());
    return new URL(
        endpoint + "?" + "filters=" + filters + "&" + "from=" + from + "&" + "size=" + size + "&sort=id&order=desc");
  }

  @SneakyThrows
  private static URL getUrl(int size, int from) {
    val endpoint = PORTAL_API + "/api/v1/repository/files";
    val filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + REPOSITORY_NAME + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + FILE_FORMAT + "\"]}}}", UTF_8.name());

    return new URL(endpoint + "?" + "filters=" + filters + "&" + "size=" + size + "&" + "from=" + from);
  }

  @SneakyThrows
  private static JsonNode read(URL url) {
    return DEFAULT.readTree(url);
  }

}
