package org.icgc.dcc.ga4gh.loader.utils.cache.id.impl;

import org.icgc.dcc.ga4gh.loader.utils.cache.id.AbstractIdCacheTemplate;
import org.icgc.dcc.ga4gh.loader.utils.cache.storage.CacheStorage;

import static com.google.common.base.Preconditions.checkState;

public class IntegerIdCache<K> extends AbstractIdCacheTemplate<K, Integer> {

  private static final Integer SINGLE_INCREMENT_AMOUNT = 1;

  public static <K> IntegerIdCache<K> newIntegerIdCache(final CacheStorage<K, Integer> cache, Integer id) {
    return new IntegerIdCache<K>(cache, id);
  }

  public IntegerIdCache(final CacheStorage<K, Integer> cache, Integer id) {
    super(cache, id);
  }

  @Override
  protected void checkIdLowerBound() {
    final Integer count = getCount();
    checkState(count >= Integer.MIN_VALUE, "The id %d must be >= %d", count, Integer.MIN_VALUE);
  }

  @Override
  protected void checkIdUpperBound() {
    final Integer count = getCount();
    checkState(count < Integer.MAX_VALUE, "The id %d must be < %d", count, Integer.MAX_VALUE);

  }

  @Override
  protected Integer incr() {
    final Integer out = getCount();
    setCount(getCount() + SINGLE_INCREMENT_AMOUNT);
    return out;
  }

}
