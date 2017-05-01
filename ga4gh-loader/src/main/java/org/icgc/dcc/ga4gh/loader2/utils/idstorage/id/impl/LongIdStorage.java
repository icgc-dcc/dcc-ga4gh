package org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.impl;

import org.icgc.dcc.ga4gh.loader2.utils.idstorage.id.AbstractIdStorageTemplate;
import org.icgc.dcc.ga4gh.loader2.utils.idstorage.storage.MapStorage;

import static com.google.common.base.Preconditions.checkState;

public class LongIdStorage<K> extends AbstractIdStorageTemplate<K, Long> {

  private static final Long SINGLE_INCREMENT_AMOUNT = 1L;

  public static <K> LongIdStorage<K> newLongIdStorage(final MapStorage<K, Long> mapStorage, final Long initId) {
    return new LongIdStorage<K>(mapStorage, initId);
  }

  public LongIdStorage(final MapStorage<K, Long> mapStorage, final Long initId) {
    super(mapStorage, initId);
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
