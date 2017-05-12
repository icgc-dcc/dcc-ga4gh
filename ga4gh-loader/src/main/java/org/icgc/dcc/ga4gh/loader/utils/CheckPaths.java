package org.icgc.dcc.ga4gh.loader.utils;

import lombok.NoArgsConstructor;

import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class CheckPaths {

  public static void checkPathExists(Path path){
    checkState(path.toFile().exists(), "The path [%s] DNE", path );
  }

  public static void checkDirPath(Path path){
    checkPathExists(path);
    checkState(path.toFile().isDirectory(), "The path [%s] is not a directory", path );
  }

  public static void checkFilePath(Path path){
    checkPathExists(path);
    checkState(path.toFile().isFile(), "The path [%s] is not a file", path );
  }

  public static void checkFilePathExtension(Path path, String ext ){
    checkFilePath(path);
    checkState(path.getFileName().endsWith(ext), "The file [%s] does not have the extension [%s]", path, ext);
  }

}
