package org.collaboratory.ga4gh.loader.model.es.converters;

import org.collaboratory.ga4gh.loader.model.contexts.FileMetaDataContext;

public interface FileMetaDataContextConverter<T> {

  T convertFromFileMetaDataContext(FileMetaDataContext fileMetaDataContext);

}
