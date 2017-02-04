package org.collaboratory.ga4gh.loader.utils.cache.impl;

import static com.google.common.base.Preconditions.checkState;
import static org.collaboratory.ga4gh.loader.utils.cache.impl.DiskCacheStorage.newDiskCacheStorage;

import java.io.IOException;

import org.collaboratory.ga4gh.loader.utils.cache.AbstractIdCache;
import org.collaboratory.ga4gh.loader.utils.cache.CacheStorage;
import org.mapdb.Serializer;

import lombok.val;

public class IntegerIdCache<K> extends AbstractIdCache<K, Integer> {

  public static <K> IntegerIdCache<K> newIntegerIdCache(final CacheStorage<K, Integer> cache, Integer id) {
    return new IntegerIdCache<K>(cache, id);
  }

  public static void main(String[] args) throws IOException {
    val idCache = IntegerIdCache.<String> newIntegerIdCache(
        newDiskCacheStorage("tester", Serializer.STRING, Serializer.INTEGER, "target", false), 1);
    idCache.add("robi");
    idCache.add("aci");
    idCache.add("baba");
    checkState(idCache.getId("robi") == 1L);
    checkState(idCache.getId("aci") == 2L);
    checkState(idCache.getId("baba") == 3L);
    System.out.println("done");

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

  @Override
  public void purge() {

  }

}
