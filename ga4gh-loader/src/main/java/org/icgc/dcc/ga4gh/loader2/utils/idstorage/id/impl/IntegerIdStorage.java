package org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl;

import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;

import static com.google.common.base.Preconditions.checkState;

public class IntegerIdStorage<K> extends AbstractIdStorageTemplate<K, Integer> {

  private static final Integer SINGLE_INCREMENT_AMOUNT = 1;

  public static <K> IntegerIdStorage<K> newIntegerIdStorage(final MapStorage<K, Integer> cache, Integer initId) {
    return new IntegerIdStorage<K>(cache, initId);
  }

  public IntegerIdStorage(final MapStorage<K, Integer> cache, Integer initId) {
    super(cache, initId);
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
