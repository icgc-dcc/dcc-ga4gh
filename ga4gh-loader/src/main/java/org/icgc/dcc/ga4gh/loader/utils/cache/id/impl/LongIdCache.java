package org.icgc.dcc.ga4gh.loader.utils.cache.id.impl;

import org.icgc.dcc.ga4gh.loader.utils.cache.id.AbstractIdCacheTemplate;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.CacheStorage;

import static com.google.common.base.Preconditions.checkState;

public class LongIdCache<K> extends AbstractIdCacheTemplate<K, Long> {

  private static final Long SINGLE_INCREMENT_AMOUNT = 1L;

  public static <K> LongIdCache<K> newLongIdCache(final CacheStorage<K, Long> cacheStorage, final Long id) {
    return new LongIdCache<K>(cacheStorage, id);
  }

  public LongIdCache(final CacheStorage<K, Long> cacheStorage, final Long id) {
    super(cacheStorage, id);
  }

  @Override
  protected void checkIdLowerBound() {
    final Long count = getCount();
    checkState(count >= Long.MIN_VALUE, "The id %d must be >= %d", count, Long.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    final Long count = getCount();
    checkState(count < Long.MAX_VALUE, "The id %d must be < %d", count, Long.MAX_VALUE);

  }

  @Override
  protected Long incr() {
    final Long out = getCount();
    setCount(getCount() + SINGLE_INCREMENT_AMOUNT);
    return out;
  }

}
