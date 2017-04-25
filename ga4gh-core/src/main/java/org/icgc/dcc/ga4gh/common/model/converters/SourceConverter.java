package org.icgc.dcc.ga4gh.common.model.converters;

import java.util.Map;

public interface SourceConverter<T> {

  T convertFromSource(Map<String, Object> source);
}
