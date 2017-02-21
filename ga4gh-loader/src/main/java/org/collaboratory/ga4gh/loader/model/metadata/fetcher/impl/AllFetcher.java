package org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;
import java.nio.file.Paths;

import static lombok.AccessLevel.PRIVATE;
import static org.collaboratory.ga4gh.loader.Portal.getAllFileMetas;
import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext.buildFileMetaDataContext;

@RequiredArgsConstructor(access = PRIVATE)
public class AllFetcher implements Fetcher {

  private static final String DEFAULT_STORAGE_FILENAME = "target/allFileMetaDatas.bin";

  @NonNull
  private String storageFilename;

  private final boolean forceNewFile;

  public static final AllFetcher newAllFetcher(final String storageFilename, final boolean forceNewFile){
    return new AllFetcher(storageFilename, forceNewFile);
  }

  public static final AllFetcher newAllFetcherDefaultStorageFilename(final boolean forceNewFile){
    return new AllFetcher(DEFAULT_STORAGE_FILENAME, forceNewFile);
  }


  @Override public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    val fromFile = Paths.get(storageFilename).toFile();
    val fileExists = fromFile.exists() && fromFile.isFile();

    FileMetaDataContext fileMetaDataContext;
    if (fileExists && !forceNewFile) {
      fileMetaDataContext = FileMetaDataContext.restore(storageFilename);
    } else {
      fileMetaDataContext = buildFileMetaDataContext(getAllFileMetas());
      fileMetaDataContext.store(storageFilename);
    }
    return fileMetaDataContext;
  }
}
