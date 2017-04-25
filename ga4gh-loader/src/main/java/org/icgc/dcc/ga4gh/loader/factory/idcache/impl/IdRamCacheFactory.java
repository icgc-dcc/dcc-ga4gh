package org.icgc.dcc.ga4gh.loader.factory.idcache.impl;

import org.icgc.dcc.ga4gh.loader.factory.idcache.AbstractIdCacheFactoryTemplate;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.impl.RamCacheStorage;

import java.io.IOException;

/*
 * All IdCaches are stored in memory
 */
public class IdRamCacheFactory extends AbstractIdCacheFactoryTemplate {

  public IdRamCacheFactory(int initId) {
    super(initId);
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    setVariantCacheStorage(RamCacheStorage.newRamCacheStorage());
    setVariantSetCacheStorage(RamCacheStorage.newRamCacheStorage());
    setCallSetCacheStorage(RamCacheStorage.newRamCacheStorage());
  }

}
