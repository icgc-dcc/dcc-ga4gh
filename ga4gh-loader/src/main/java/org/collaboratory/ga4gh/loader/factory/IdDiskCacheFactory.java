package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.cache.impl.DiskCacheStorage.newDiskCacheStorage;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.mapdb.Serializer;

/*
 * All IdCaches are stored on disk via MapDB
 */
public final class IdDiskCacheFactory extends AbstractIdCacheFactory {

  private final String storageDirname;

  public IdDiskCacheFactory(final int initId, final String storageDirname) {
    super(initId);
    this.storageDirname = storageDirname;
  }

  @Override
  protected void buildCacheStorage() throws IOException {
    variantCacheStorage = newDiskCacheStorage("variantIdCache", new EsVariant.EsVariantSerializer(), Serializer.LONG,
        storageDirname, false);
    variantSetCacheStorage =
        newDiskCacheStorage("variantSetIdCache", Serializer.STRING, Serializer.INTEGER,
            storageDirname, false);
    callSetCacheStorage =
        newDiskCacheStorage("variantSetIdCache", Serializer.STRING, Serializer.INTEGER,
            storageDirname, false);
  }

  @Override
  public void close() throws IOException {
    variantCacheStorage.close();
    variantSetCacheStorage.close();
    callSetCacheStorage.close();
  }

}
