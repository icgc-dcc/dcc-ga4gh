package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.cache.impl.RamCacheStorage.newRamCacheStorage;

import java.io.IOException;

/*
 * All IdCaches are stored in memory
 */
public class IdRamCacheFactory extends AbstractIdCacheFactory {

  public IdRamCacheFactory(int initId) {
    super(initId);
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  protected void buildCacheStorage() throws IOException {
    this.variantCacheStorage = newRamCacheStorage();
    this.variantSetCacheStorage = newRamCacheStorage();
    this.callSetCacheStorage = newRamCacheStorage();
  }

}
