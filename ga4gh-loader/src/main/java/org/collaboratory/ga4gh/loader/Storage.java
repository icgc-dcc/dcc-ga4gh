package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.concurrent.NotThreadSafe;

import org.collaboratory.ga4gh.loader.metadata.FileMetaData;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.hash.Hashing;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
@NotThreadSafe
public final class Storage {

  private final boolean persist;

  private final Path outputDir;

  private final long currentTime;

  private final Path tempFile;

  private static final String tempFilename = "tmp.vcf.gz";

  public Storage(final boolean persist, @NonNull final String outputDirName) {
    this.persist = persist;
    this.outputDir = Paths.get(outputDirName).toAbsolutePath();
    initDir(outputDir);
    this.currentTime = System.currentTimeMillis();
    this.tempFile = outputDir.resolve(tempFilename);
  }

  @SneakyThrows
  private static void initDir(@NonNull final Path dir) {
    val dirDoesNotExist = !Files.exists(dir);
    if (dirDoesNotExist) {
      Files.createDirectories(dir);
    }
  }

  private void checkForParentDir(@NonNull Path file) {
    if (!file.isAbsolute()) {
      file = file.toAbsolutePath();
    }
    checkState(file.startsWith(outputDir),
        "The file [%s] must have the parent directory [%s] in its path",
        file, outputDir);
  }

  // Used for subdirectories inside outputDir
  private static void initParentDir(@NonNull Path file) {
    val parentDir = file.getParent();
    initDir(parentDir);
  }

  // Download file regardless of persist mode
  @SneakyThrows
  private static File downloadFileByObjectId(@NonNull final String objectId, @NonNull final String filename) {
    val objectUrl = getObjectUrl(objectId);
    val output = Paths.get(filename);

    @Cleanup
    val input = objectUrl.openStream();
    copy(input, output, REPLACE_EXISTING);

    return output.toFile();
  }

  @SneakyThrows
  public File downloadFile(@NonNull final FileMetaData fileMetaData) {
    val objectId = fileMetaData.getObjectId();
    val expectedMD5Sum = fileMetaData.getFileMd5sum();
    val relativeFilename = fileMetaData.getVcfFilenameParser().getFilename();
    val relativeFile = Paths.get(relativeFilename);
    val absFile = outputDir.resolve(relativeFile).toAbsolutePath();
    val absFilename = absFile.toString();
    checkForParentDir(absFile);
    initParentDir(absFile);
    val fileExists = Files.exists(absFile);
    val md5Match = fileExists && calcMd5Sum(absFile).equals(expectedMD5Sum); // Short circuit
    if (persist) {
      if (md5Match) {
        log.info("File [{}] already exists and matches checksum. Skipping download.", absFile);
        return absFile.toFile();
      } else {
        return downloadFileByObjectId(objectId, absFilename);
      }
    } else {
      return downloadFileByObjectId(objectId, tempFile.toAbsolutePath().toString());
    }
  }

  private static String calcMd5Sum(@NonNull final Path file) throws IOException {
    checkState(file.toFile().isFile(), "The input path [%s] is not a file", file);
    val bytes = Files.readAllBytes(file);
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
