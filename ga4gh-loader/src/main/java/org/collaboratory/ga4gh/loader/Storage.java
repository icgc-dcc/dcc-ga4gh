package org.collaboratory.ga4gh.loader;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.nio.file.Files.copy;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_API;
import static org.collaboratory.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

public class Storage {

  @SneakyThrows
  public static File downloadFile(String objectId) {
    val objectUrl = getObjectUrl(objectId);
    val output = Paths.get("/tmp/file.vcf.gz");

    @Cleanup
    val input = objectUrl.openStream();
    copy(input, output, REPLACE_EXISTING);

    return output.toFile();
  }

  private static URL getObjectUrl(String objectId) throws IOException {
    val storageUrl = new URL(STORAGE_API + "/download/" + objectId + "?offset=0&length=-1&external=true");

    val connection = (HttpURLConnection) storageUrl.openConnection();
    connection.setRequestProperty(AUTHORIZATION, "Bearer " + TOKEN);

    val object = readObject(connection);

    return getUrl(object);
  }

  private static URL getUrl(JsonNode object) throws MalformedURLException {
    return new URL(object.get("parts").get(0).get("url").textValue());
  }

  private static JsonNode readObject(HttpURLConnection connection) throws JsonProcessingException, IOException {
    return DEFAULT.readTree(connection.getInputStream());
  }

}
