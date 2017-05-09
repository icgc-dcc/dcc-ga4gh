package org.icgc.dcc.ga4gh.common.model.converters;

import org.elasticsearch.search.SearchHit;

import java.util.Map;

public interface SearchHitConverter<T> {

  default T convertFromSearchHit(SearchHit hit){
    return convertFromSource(hit.getSource());
  }

  T convertFromSource(Map<String, Object> source);

}
