package org.icgc.dcc.ga4gh.loader2.persistance;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.ga4gh.loader2.persistance.LocalFileRestorer.newLocalFileRestorer;

public class LocalFileRestorerFactory {

  private static final String DEFAULT_EXT = "dat";

  public static final LocalFileRestorerFactory newFileRestorerFactory(Path outputDir){
    return new LocalFileRestorerFactory(outputDir);
  }
  public static final LocalFileRestorerFactory newFileRestorerFactory(String outputDirname){
    return newFileRestorerFactory(Paths.get(outputDirname));
  }

  @NonNull private final Path outputDir;

  @SneakyThrows
  public LocalFileRestorerFactory(Path outputDir){
    this.outputDir = outputDir;
    initDir(outputDir);
  }

  private static void initDir(Path outputDir) throws IOException {
    if (!outputDir.toFile().exists()){
      Files.createDirectories(outputDir);
    }
  }

  public <T extends Serializable> FileRestorer<Path, T>  createFileRestorer(String filename){
    val path = outputDir.resolve(filename);
    return newLocalFileRestorer(path);
  }

  public <T extends Serializable> FileRestorer<Path, T>  createFileRestorerByName(String objectName){
    return createFileRestorer(DOT.join(objectName,DEFAULT_EXT));
  }

}
