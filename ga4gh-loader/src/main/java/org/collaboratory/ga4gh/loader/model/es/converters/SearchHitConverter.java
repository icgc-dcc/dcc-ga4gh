package org.collaboratory.ga4gh.loader.model.es.converters;

import org.elasticsearch.search.SearchHit;

public interface SearchHitConverter<T> {

  T convertFromSearchHit(SearchHit hit);

}
