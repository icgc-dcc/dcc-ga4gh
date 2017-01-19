package org.collaboratory.ga4gh.loader.factory;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.utils.IdDiskCache;

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
  private IdDiskCache variantIdCache;
  @NonFinal
  private IdDiskCache variantSetIdCache;
  @NonFinal
  private IdDiskCache callSetIdCache;
  @NonFinal
  private IdDiskCache callIdCache;

  @Override
  public IdCacheFactory init() throws IOException {
    variantIdCache = new IdDiskCache("variantIdCache", storageDirname, initId);
    variantSetIdCache = new IdDiskCache("variantSetIdCache", storageDirname, initId);
    callSetIdCache = new IdDiskCache("callSetIdCache", storageDirname, initId);
    callIdCache = new IdDiskCache("callIdCache", storageDirname, initId);
    return this;
  }

  @Override
  public void close() throws IOException {
    variantSetIdCache.close();
    variantIdCache.close();
    callIdCache.close();
    callSetIdCache.close();
  }

  @Override
  public void purge() {
    variantSetIdCache.purge();
    variantIdCache.purge();
    callIdCache.purge();
    callSetIdCache.purge();
  }
}
