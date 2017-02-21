package org.collaboratory.ga4gh.loader.model.metadata.fetcher.decorators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;

@RequiredArgsConstructor
public class LimitFetcherDecorator implements Fetcher {

  public static LimitFetcherDecorator newLimitFetcherDecorator(final Fetcher fetcher, final int limit){
    return new LimitFetcherDecorator(fetcher, limit);
  }

  @NonNull
  private final Fetcher fetcher;

  private final int limit;

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    val fileMetaDataContext = fetcher.fetch();
    val size = fileMetaDataContext.size();
    val subFileMetaDataList = fileMetaDataContext.getFileMetaDatas().subList(0, Math.min(limit, size));
    return FileMetaDataContext.builder().fileMetaDatas(subFileMetaDataList).build();
  }
}
