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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.hash.Hashing;

import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = PRIVATE)
@Slf4j
public final class Storage {

  @SneakyThrows
  public static File downloadFile(@NonNull final String objectId, @NonNull final String filename) {
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
  public static File downloadFileAndPersist(@NonNull final String objectId, @NonNull final String filename,
      @NonNull final String expectedMD5Sum) {
    val path = Paths.get(filename);
    Path dir = path.getParent();
    if (dir == null) {
      dir = Paths.get("./");
    }
    val dirDoesNotExist = !Files.exists(dir);
    if (dirDoesNotExist) {
      Files.createDirectories(dir);
    }
    if (Files.exists(path)) {
      val md5Mismatch = !calcMd5Sum(filename).equals(expectedMD5Sum);
      if (md5Mismatch) {
        return downloadFile(objectId, filename);
      } else {
        log.info("File [{}] already exists and matches checksum. Skipping download.", filename);
        return path.toFile();
      }
    } else {
      return downloadFile(objectId, filename);
    }
  }

  private static String calcMd5Sum(final String filename) throws IOException {
    val path = Paths.get(filename);
    val bytes = Files.readAllBytes(path);
    return Hashing.md5()
        .newHasher()
        .putBytes(bytes)
        .hash()
        .toString();
  }

  private static URL getObjectUrl(final String objectId) throws IOException {
    val storageUrl = new URL(STORAGE_API + "/download/" + objectId + "?offset=0&length=-1&external=true");
    val connection = (HttpURLConnection) storageUrl.openConnection();
    connection.setRequestProperty(AUTHORIZATION, "Bearer " + TOKEN);
    val object = readObject(connection);
    return getUrl(object);
  }

  private static URL getUrl(@NonNull final JsonNode object) throws MalformedURLException {
    return new URL(object.get("parts").get(0).get("url").textValue());
  }

  private static JsonNode readObject(@NonNull final HttpURLConnection connection)
      throws JsonProcessingException, IOException {
    return DEFAULT.readTree(connection.getInputStream());
  }
}
