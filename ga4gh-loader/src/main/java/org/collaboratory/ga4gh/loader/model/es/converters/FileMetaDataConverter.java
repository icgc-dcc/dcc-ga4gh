package org.collaboratory.ga4gh.loader.model.es.converters;

import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;

public interface FileMetaDataConverter<T> {

  T convertFromFileMetaData(FileMetaData fileMetaData);

}
