package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Config.PORTAL_API;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import lombok.NoArgsConstructor;
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

  }

  private static List<FileMetaData> getAllFileMetaDatasForDonor(String donorId) {
    return getFileMetaDatasForDonor(donorId, 1, -1);
  }

  private static List<FileMetaData> getFileMetaDatasForDonor(String donorId, final int startFrom, final int numFiles) {
    val samplesFileMetaDataList = new ArrayList<FileMetaData>();
    int fileFrom = startFrom;
    while (true) {
      // If numFiles < 0, then get all
      int buffFileSize =
          (numFiles < 0) ? DEFAULT_BUF_FILE_SIZE : Math.min(startFrom + numFiles - fileFrom, DEFAULT_BUF_FILE_SIZE);
      val url = getFilesForDonerUrl(donorId, fileFrom, buffFileSize);
      val result = read(url);
      val hits = getHits(result);
      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        samplesFileMetaDataList.add(FileMetaData.build(fileMeta));
      }

      if (hits.size() < buffFileSize) {
        break;
      }
      fileFrom += buffFileSize;
    }

    return samplesFileMetaDataList;
  }

  /**
   * Gets all Collaboratory VCF files.
   */
  public static List<ObjectNode> getAllFileMetas() {
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

    val s = fileMetas.build();

    return s;
  }

  public static Map<String, DonorData> getDonorDataMap() {
    return DonorData.buildDonorDataMap(getAllFileMetas());
  }

  public static Map<String, DonorData> getDonorDataMap(int numDonors) {
    return DonorData.buildDonorDataMap(getFileMetasForNumDonors(numDonors));
  }

  public static List<ObjectNode> getFileMetasForNumDonors(int numDonors) {
    checkState(numDonors > 1); // due to bug in Portal api, must be greater than 1
    val fileMetas = ImmutableList.<ObjectNode> builder();

    int from = 1;
    val donorIterable = getDonorIds(from, numDonors);

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
  private static Iterable<String> getDonorIds(final int startPos, final int numDonors) {
    checkState(numDonors > 0);
    checkState(startPos > 0);
    val url = getDonersUrl(numDonors, startPos);
    val result = read(url);
    val hits = getHits(result);
    return transform(hits, Portal::getIdFromHit);
  }

  public static Iterable<String> getDonorIds() {
    int from = 1;
    int size = DEFAULT_BUF_DONOR_SIZE;
    val list = new ArrayList<String>();
    while (true) {
      val donorList = getDonorIds(from, size);
      Iterables.addAll(list, donorList);
      if (Iterables.size(donorList) < size) {
        break;
      }
      from += size;
    }
    return list;
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
