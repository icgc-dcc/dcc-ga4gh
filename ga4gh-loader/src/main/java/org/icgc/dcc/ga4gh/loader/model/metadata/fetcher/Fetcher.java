package org.icgc.dcc.ga4gh.loader.model.metadata.fetcher;

import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;

import java.io.IOException;

public interface Fetcher {
  FileMetaDataContext fetch() throws IOException, ClassNotFoundException;
}
