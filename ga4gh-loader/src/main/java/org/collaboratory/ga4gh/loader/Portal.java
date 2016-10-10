package org.collaboratory.ga4gh.loader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.collaboratory.ga4gh.loader.Config.PORTAL_API;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;
import lombok.val;

public class Portal {

  public static List<String> getObjectIds() {
    val objectIds = ImmutableList.<String> builder();
    val size = 100;

    int from = 1;
    while (true) {
      val url = getUrl(size, from);
      val result = read(url);
      val hits = getHits(result);

      for (val hit : hits) {
        val objectId = getObjectId(hit);
        objectIds.add(objectId);
      }

      if (hits.size() < size) {
        break;
      }

      from += size;
    }

    return objectIds.build();
  }

  private static JsonNode getHits(JsonNode result) {
    return result.get("hits");
  }

  private static String getObjectId(JsonNode hit) {
    return hit.get("objectId").textValue();
  }

  @SneakyThrows
  private static URL getUrl(int size, int from) {
    val endpoint = PORTAL_API + "/api/v1/repository/files";
    val repository = "Collaboratory - Toronto";
    val fileFormat = "VCF";
    val filters = URLEncoder.encode("{\"file\":{\"repoName\":{\"is\":[\"" + repository + "\"]},"
        + "\"fileFormat\":{\"is\":[\"" + fileFormat + "\"]}}}", UTF_8.name());

    return new URL(endpoint + "?" + "filters=" + filters + "&" + "size=" + size + "&" + "from=" + from);
  }

  @SneakyThrows
  private static JsonNode read(URL url) {
    return DEFAULT.readTree(url);
  }

}
