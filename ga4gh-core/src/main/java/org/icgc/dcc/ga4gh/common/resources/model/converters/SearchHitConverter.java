package org.icgc.dcc.ga4gh.common.resources.model.converters;

import org.elasticsearch.search.SearchHit;

public interface SearchHitConverter<T> {

  T convertFromSearchHit(SearchHit hit);

}
