package org.icgc.dcc.ga4gh.loader.factory.idcache.impl;

import org.icgc.dcc.ga4gh.common.model.es.EsVariant;
import org.icgc.dcc.ga4gh.loader.factory.idcache.AbstractIdCacheFactoryTemplate;
import org.mapdb.Serializer;

import java.io.IOException;

import static org.icgc.dcc.ga4gh.loader.utils.cache.storage.impl.DiskCacheStorage.newDiskCacheStorage;

/*
 * All IdCaches are stored on disk via MapDB
 */
public final class IdDiskCacheFactory extends AbstractIdCacheFactoryTemplate {

  private final String storageDirname;

  public IdDiskCacheFactory(final int initId, final String storageDirname) {
    super(initId);
    this.storageDirname = storageDirname;
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    setVariantCacheStorage(newDiskCacheStorage("variantIdCache", new EsVariant.EsVariantSerializer(), Serializer.LONG,
        storageDirname, false));
    setVariantSetCacheStorage(newDiskCacheStorage("variantSetIdCache", Serializer.STRING, Serializer.INTEGER,
            storageDirname, false));
    setCallSetCacheStorage(newDiskCacheStorage("variantSetIdCache", Serializer.STRING, Serializer.INTEGER,
            storageDirname, false));
  }

}
