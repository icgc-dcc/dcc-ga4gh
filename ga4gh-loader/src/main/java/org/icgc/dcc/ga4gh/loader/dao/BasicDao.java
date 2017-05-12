package org.icgc.dcc.ga4gh.loader.dao;

import java.util.List;

public interface BasicDao<B, R> {

  List<B> find(R request);

  List<B> findAll();

}
