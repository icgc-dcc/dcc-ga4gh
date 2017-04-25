package org.icgc.dcc.ga4gh.loader.model.metadata.converters;

import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaDataContext;

public interface FileMetaDataContextConverter<T> {

  T convertFromFileMetaDataContext(FileMetaDataContext fileMetaDataContext);

}
