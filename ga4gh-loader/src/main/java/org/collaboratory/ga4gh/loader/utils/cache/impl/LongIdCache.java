package org.collaboratory.ga4gh.loader.utils.cache.impl;

import static com.google.common.base.Preconditions.checkState;

import org.collaboratory.ga4gh.loader.utils.cache.AbstractIdCache;
import org.collaboratory.ga4gh.loader.utils.cache.CacheStorage;

import lombok.val;

public class LongIdCache<K> extends AbstractIdCache<K, Long> {

  public static <K> LongIdCache<K> newLongIdCache(final CacheStorage<K, Long> cacheStorage, final Long id) {
    return new LongIdCache<K>(cacheStorage, id);
  }

  public LongIdCache(final CacheStorage<K, Long> cacheStorage, final Long id) {
    super(cacheStorage, id);
  }

  @Override
  protected void checkIdLowerBound() {
    val count = getCount();
    checkState(count >= Long.MIN_VALUE, "The id %d must be >= %d", count, Long.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    val count = getCount();
    checkState(count < Long.MAX_VALUE, "The id %d must be < %d", count, Long.MAX_VALUE);

  }

  @Override
  protected Long incr() {
    val out = getCount();
    setCount(getCount() + 1);
    return out;
  }

}
