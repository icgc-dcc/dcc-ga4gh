package org.collaboratory.ga4gh.loader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Config.PORTAL_API;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class Portal {

  private static final int PORTAL_FETCH_SIZE = 100;
  private static final String REPOSITORY_NAME = "Collaboratory - Toronto";
  private static final String FILE_FORMAT = "VCF";

  /**
   * Gets all Collaboratory VCF files.
   */
  public static List<ObjectNode> getFileMetas() {
    val fileMetas = ImmutableList.<ObjectNode> builder();
    val size = 2;// PORTAL_FETCH_SIZE;
    int from = 1;
    while (from < 2) {
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

  private static JsonNode getHits(JsonNode result) {
    return result.get("hits");
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
