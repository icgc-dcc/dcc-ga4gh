package org.collaboratory.ga4gh.loader.model.metadata.converters;

import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;

public interface FileMetaDataConverter<T> {

  T convertFromFileMetaData(FileMetaData fileMetaData);

}
