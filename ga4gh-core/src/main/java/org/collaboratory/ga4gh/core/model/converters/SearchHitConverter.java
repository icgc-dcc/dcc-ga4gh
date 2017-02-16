package org.collaboratory.ga4gh.core.model.converters;

import org.elasticsearch.search.SearchHit;

public interface SearchHitConverter<T> {

  T convertFromSearchHit(SearchHit hit);

}
