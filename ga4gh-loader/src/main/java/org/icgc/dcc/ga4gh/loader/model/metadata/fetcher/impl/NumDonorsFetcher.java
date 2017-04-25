package org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.impl;

import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.loader.Portal;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.icgc.dcc.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;

@RequiredArgsConstructor
public class NumDonorsFetcher implements Fetcher {

  public static NumDonorsFetcher newNumDonorsFetcher(final int numDonors){
    return new NumDonorsFetcher(numDonors);
  }

  private final int numDonors;

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    return FileMetaDataContext.buildFileMetaDataContext(Portal.getFileMetasForNumDonors(numDonors));
  }
}
