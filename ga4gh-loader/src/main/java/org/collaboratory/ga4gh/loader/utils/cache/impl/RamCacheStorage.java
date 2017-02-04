package org.collaboratory.ga4gh.loader.utils.cache.impl;

import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;
import java.util.Map;

import org.collaboratory.ga4gh.loader.utils.cache.CacheStorage;

public class RamCacheStorage<K, V> implements CacheStorage<K, V> {

  private Map<K, V> map;

  public static <K, V> RamCacheStorage<K, V> newRamCacheStorage() {
    return new RamCacheStorage<K, V>();
  }

  public RamCacheStorage() {
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
