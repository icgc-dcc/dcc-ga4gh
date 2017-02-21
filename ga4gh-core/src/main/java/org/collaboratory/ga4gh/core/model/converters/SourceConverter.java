package org.collaboratory.ga4gh.core.model.converters;

import java.util.Map;

public interface SourceConverter<T> {

  T convertFromSource(Map<String, Object> source);
}
