package org.icgc.dcc.ga4gh.loader.utils.idstorage.context;

import java.util.List;

public interface IdStorageContext<ID, K> {

  void add(K object);

  void addAll(List<K> objects);

  ID getId();

  List<K> getObjects();
}
