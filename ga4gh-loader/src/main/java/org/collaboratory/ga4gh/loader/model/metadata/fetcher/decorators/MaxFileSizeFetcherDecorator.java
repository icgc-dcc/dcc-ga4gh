package org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataFilters;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;

@RequiredArgsConstructor
public class MaxFileSizeFetcherDecorator implements Fetcher {

  public static MaxFileSizeFetcherDecorator newMaxFileSizeFetcherDecorator(final Fetcher fetcher, final int maxFileSizeBytes){
    return new MaxFileSizeFetcherDecorator(fetcher, maxFileSizeBytes);
  }

  @NonNull
  private final Fetcher fetcher;

  private final int maxFileSizeBytes;

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    val fileMetaDataContext = fetcher.fetch();
    return FileMetaDataFilters.filterBySize(fileMetaDataContext, maxFileSizeBytes);
  }
}
