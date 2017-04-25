package org.icgc.dcc.ga4gh.common.resources.model.converters;

import java.util.Map;

public interface SourceConverter<T> {

  T convertFromSource(Map<String, Object> source);
}
