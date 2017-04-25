package org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.decorators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.Fetcher;

@RequiredArgsConstructor
public class SSMFetcherDecorator implements Fetcher {
  private static final String SSM_DATA_TYPE = "SSM";

  public static SSMFetcherDecorator newSSMFetcherDecorator(final Fetcher fetcher){
    return new SSMFetcherDecorator(fetcher);
  }

  @NonNull
  private final Fetcher fetcher;

  @Override
  @SneakyThrows
  public FileMetaDataContext fetch() {
    val fileMetaDataContext = fetcher.fetch();
    return fileMetaDataContext.filter(x -> x.getDataType().equals(SSM_DATA_TYPE));
  }
}
