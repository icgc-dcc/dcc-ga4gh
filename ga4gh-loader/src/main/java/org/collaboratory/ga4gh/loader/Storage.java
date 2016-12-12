package org.collaboratory.ga4gh.loader;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.nio.file.Files.copy;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Config.STORAGE_API;
import static org.collaboratory.ga4gh.loader.Config.TOKEN;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.shaded.apache.commons.codec.digest.DigestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = PRIVATE)
@Slf4j
public final class Storage {

  public static void main(String[] args) throws Exception {
    String filename = "target/rob_fileMetasFor20donors.txt";
    log.info("Filename: {}   Md5Sum: {}", filename, calcMd5Sum(filename));
  }

  public static void main2(String[] args) {
    Map<String, String> map = new HashMap<>();
    map.put("FI671314", "035fba3f-dfef-50be-9f43-0b3831fa983f");
    map.put("FI671208", "6df08e83-a6e2-5ea5-896b-850b195cf991");
    for (val entry : map.entrySet()) {
      String outputFn = "/tmp/rob." + entry.getKey() + ".vcf.gz";
      String objectId = entry.getValue();
      try {
        downloadFile(objectId, outputFn);
        System.out.println("Successfully downloaded file: " + outputFn);
      } catch (Exception e) {
        System.out.println("Failed to download " + outputFn + ":  " + e.getMessage());
      }

    }
  }

  @SneakyThrows
  public static File downloadFile(final String objectId, final String filename) {
    val objectUrl = getObjectUrl(objectId);
    val output = Paths.get(filename);

    @Cleanup
    val input = objectUrl.openStream();
    copy(input, output, REPLACE_EXISTING);

    return output.toFile();
  }

  @SneakyThrows
  public static File downloadFile(final String objectId) {
    return downloadFile(objectId, "/tmp/file.vcf.gz");
  }

  @SneakyThrows
  public static File downloadFileAndPersist(final String objectId, final String filename, final String expectedMD5Sum) {
    val path = Paths.get(filename);
    val dir = path.getParent();
    if (Files.exists(dir) == false) {
      Files.createDirectories(dir);
    }
    if (Files.exists(path)) {
      if (calcMd5Sum(filename).equals(expectedMD5Sum) == false) {
        return downloadFile(objectId, filename);
      } else {
        log.info("File \"{}\" already exists and matches checksum. Skipping download.", filename);
        return path.toFile();
      }
    } else {
      return downloadFile(objectId, filename);
    }
  }

  private static String calcMd5Sum(final String filename) throws IOException {
    val path = Paths.get(filename);
    InputStream is = Files.newInputStream(path);
    return DigestUtils.md5Hex(is);
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
