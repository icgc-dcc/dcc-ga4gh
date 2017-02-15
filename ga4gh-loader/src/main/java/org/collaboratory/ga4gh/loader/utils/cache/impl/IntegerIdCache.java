package org.collaboratory.ga4gh.loader.utils.cache.impl;

import static com.google.common.base.Preconditions.checkState;

import org.collaboratory.ga4gh.loader.utils.cache.AbstractIdCacheTemplate;
import org.collaboratory.ga4gh.loader.utils.cache.CacheStorage;

import lombok.val;

public class IntegerIdCache<K> extends AbstractIdCacheTemplate<K, Integer> {

  public static <K> IntegerIdCache<K> newIntegerIdCache(final CacheStorage<K, Integer> cache, Integer id) {
    return new IntegerIdCache<K>(cache, id);
  }

  public IntegerIdCache(final CacheStorage<K, Integer> cache, Integer id) {
    super(cache, id);
  }

  @Override
  protected void checkIdLowerBound() {
    val count = getCount();
    checkState(count >= Integer.MIN_VALUE, "The id %d must be >= %d", count, Integer.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    val count = getCount();
    checkState(count < Integer.MAX_VALUE, "The id %d must be < %d", count, Integer.MAX_VALUE);

  }

  @Override
  protected Integer incr() {
    val out = getCount();
    setCount(getCount() + 1);
    return out;
  }

}
