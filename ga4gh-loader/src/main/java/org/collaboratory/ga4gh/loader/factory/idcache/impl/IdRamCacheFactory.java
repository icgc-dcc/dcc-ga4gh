package org.collaboratory.ga4gh.loader.factory.idcache.impl;

import org.collaboratory.ga4gh.loader.factory.idcache.AbstractIdCacheFactoryTemplate;

import java.io.IOException;

import static org.collaboratory.ga4gh.loader.utils.cache.storage.impl.RamCacheStorage.newRamCacheStorage;

/*
 * All IdCaches are stored in memory
 */
public class IdRamCacheFactory extends AbstractIdCacheFactoryTemplate {

  public IdRamCacheFactory(int initId) {
    super(initId);
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    setVariantCacheStorage(newRamCacheStorage());
    setVariantSetCacheStorage(newRamCacheStorage());
    setCallSetCacheStorage(newRamCacheStorage());
  }

}
