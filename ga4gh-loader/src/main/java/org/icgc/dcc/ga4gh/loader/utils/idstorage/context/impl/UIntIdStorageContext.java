package org.icgc.dcc.ga4gh.loader.utils.idstorage.context.impl;

import org.icgc.dcc.ga4gh.loader.utils.idstorage.context.IdStorageContext;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class UIntIdStorageContext<T> implements IdStorageContext<Long,T> {

  private static final long MAX_UINT = (1L << 32) - 1;
  private static final long MIN_UINT = 0L;

  public static <T> UIntIdStorageContext<T> createUIntIdStorageContext(long id ){
    return new UIntIdStorageContext<T>(id);
  }

  public static <T> UIntIdStorageContext<T> createUIntIdStorageContext(long id, List<T> objects ){
    return new UIntIdStorageContext<T>(id, objects);
  }

  private final int uintId;
  private final List<T> objects;

  public UIntIdStorageContext(long id, List<T> objects) {
    checkArgument(id <= MAX_UINT && id >= MIN_UINT,  "The input ID [%s] must be uint, or between %s and %s", id, MIN_UINT, MAX_UINT);
    this.uintId = (int)(id + Integer.MIN_VALUE); // convertBasic to uint integer
    this.objects = objects;
  }

  public UIntIdStorageContext(long id) {
    this(id, newArrayList());
  }

  @Override public void add(T object) {
    objects.add(object);
  }

  @Override public void addAll(List<T> objects) {
    this.objects.addAll(objects);
  }

  @Override public Long getId() {
    return (long)uintId - Integer.MIN_VALUE ; // convertBasic back to long
  }

  @Override public List<T> getObjects() {
    return objects;
  }

}
