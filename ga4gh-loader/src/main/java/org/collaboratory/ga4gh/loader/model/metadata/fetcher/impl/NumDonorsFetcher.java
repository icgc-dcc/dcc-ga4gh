package org.collaboratory.ga4gh.loader.model.metadata.fetcher.impl;

import lombok.RequiredArgsConstructor;
import org.collaboratory.ga4gh.loader.Portal;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext;
import org.collaboratory.ga4gh.loader.model.metadata.fetcher.Fetcher;

import java.io.IOException;

import static org.collaboratory.ga4gh.loader.model.metadata.FileMetaDataContext.buildFileMetaDataContext;

@RequiredArgsConstructor
public class NumDonorsFetcher implements Fetcher {

  public static NumDonorsFetcher newNumDonorsFetcher(final int numDonors){
    return new NumDonorsFetcher(numDonors);
  }

  private final int numDonors;

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    return buildFileMetaDataContext(Portal.getFileMetasForNumDonors(numDonors));
  }
}
