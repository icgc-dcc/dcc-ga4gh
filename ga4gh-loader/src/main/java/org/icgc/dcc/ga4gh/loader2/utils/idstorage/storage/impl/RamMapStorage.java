package org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.impl;

import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class RamMapStorage<K, V> implements MapStorage<K, V> {

  private Map<K, V> map;

  public static <K, V> RamMapStorage<K, V> newRamMapStorage() {
    return new RamMapStorage<K, V>();
  }

  public RamMapStorage() {
    this.map = newHashMap();
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public void purge() {
    map = newHashMap();
  }

  @Override
  public Map<K, V> getMap() {
    return map;
  }

}
