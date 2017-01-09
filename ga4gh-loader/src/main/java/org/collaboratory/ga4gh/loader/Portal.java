package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Config.PORTAL_API;
import static org.collaboratory.ga4gh.loader.metadata.FileMetaData.buildFileMetaDataList;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.ID;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import org.collaboratory.ga4gh.loader.metadata.FileMetaData;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class Portal {

  /*
   * Constants
   */
  private static final String REPOSITORY_FILES_ENDPOINT = "/api/v1/repository/files";
  private static final String DONORS_ENDPOINT = "/api/v1/donors";
  private static final int PORTAL_FETCH_SIZE = 100;
  private static final String REPOSITORY_NAME = "Collaboratory - Toronto";
  private static final String FILE_FORMAT = "VCF";
  private static final int DEFAULT_BUF_DONOR_SIZE = 50;
  private static final String HITS = "hits";

  /**
   * Gets all Collaboratory VCF files.
   */
  public static List<ObjectNode> getAllFileMetas() {
    val fileMetas = ImmutableList.<ObjectNode> builder();
    val size = PORTAL_FETCH_SIZE;
    int from = 1;

    while (true) {
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

  public static List<FileMetaData> getFileMetaDatasForNumDonors(int numDonors) {
    return buildFileMetaDataList(getFileMetasForNumDonors(numDonors));
  }

  public static List<FileMetaData> getAllFileMetaDatas() {
    return buildFileMetaDataList(getAllFileMetas());
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
    return hit.path(ID).textValue();
  }

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
    val list = ImmutableList.<String> builder();
    while (true) {
      val donorList = getDonorIds(from, size);
      list.addAll(donorList);
      if (Iterables.size(donorList) < size) {
        break;
      }
      from += size;
    }
    return list.build();
  }

  private static JsonNode getHits(JsonNode result) {
    return result.get(HITS);
  }

  @SneakyThrows
  private static URL getDonersUrl(int size, int from) {
    val endpoint = PORTAL_API + DONORS_ENDPOINT;
    return new URL(endpoint + "?" + "from=" + from + "&size=" + size + "&order=desc&facetsOnly=false");
  }

  // TODO: [rtisma] -- need to updated this. When request N donors, sometimes get < N donors back. Make sure filtering
  // correctly for SSM only
  @SneakyThrows
  private static URL getFilesForDonersUrl(@NonNull Iterable<String> donorIterable, final int size, final int from) {
    val endpoint = PORTAL_API + REPOSITORY_FILES_ENDPOINT;

    String donorsCSV = Joiner.on("\",\"").join(donorIterable);
    // {"file":{"repoName":{"is":["Collaboratory - Toronto"]},"fileFormat":{"is":["VCF"]},"donorId":{"is":["DO222843"]}}
    String filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + REPOSITORY_NAME + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + FILE_FORMAT + "\"]},"
        + "\"donorId\":{\"is\":[\"" + donorsCSV + "\"]}}}", UTF_8.name());
    return new URL(
        endpoint + "?" + "filters=" + filters + "&" + "from=" + from + "&" + "size=" + size + "&sort=id&order=desc");
  }

  @SneakyThrows
  private static URL getUrl(int size, int from) {
    val endpoint = PORTAL_API + REPOSITORY_FILES_ENDPOINT;
    val filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + REPOSITORY_NAME + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + FILE_FORMAT + "\"]}}}", UTF_8.name());

    return new URL(endpoint + "?" + "filters=" + filters + "&" + "size=" + size + "&" + "from=" + from);
  }

  @SneakyThrows
  private static JsonNode read(URL url) {
    return DEFAULT.readTree(url);
  }
}
