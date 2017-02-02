package org.collaboratory.ga4gh.loader.factory;

import static org.collaboratory.ga4gh.loader.utils.IdDiskCache.newIdDiskCache;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSerializer;
import org.collaboratory.ga4gh.loader.utils.IdDiskCache;
import org.mapdb.Serializer;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;

@RequiredArgsConstructor
@Value
public class IdDiskCacheFactory implements IdCacheFactory {

  private final String storageDirname;
  private final long initId;

  // State
  @NonFinal
  private IdDiskCache<EsVariant> variantIdCache;

  @NonFinal
  private IdDiskCache<String> variantSetIdCache;

  @NonFinal
  private IdDiskCache<String> callSetIdCache;

  @Override
  public void build() throws IOException {
    variantIdCache =
        newIdDiskCache("variantIdCache", new EsVariantSerializer(), storageDirname,
            initId);
    variantSetIdCache = newIdDiskCache("variantSetIdCache", Serializer.STRING_ASCII, storageDirname, initId);
    callSetIdCache = newIdDiskCache("callSetIdCache", Serializer.STRING_ASCII, storageDirname, initId);
  }

  @Override
  public void close() throws IOException {
    variantSetIdCache.close();
    variantIdCache.close();
    callSetIdCache.close();
  }

  @Override
  public void purge() {
    variantSetIdCache.purge();
    variantIdCache.purge();
    callSetIdCache.purge();
  }

}
