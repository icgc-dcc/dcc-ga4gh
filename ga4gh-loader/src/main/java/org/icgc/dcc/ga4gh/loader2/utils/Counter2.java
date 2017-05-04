package org.icgc.dcc.ga4gh.loader2.utils;

public interface Counter2<T> {

  T incr();

  T incr(T amount);

  void reset();

  T getCount();

  T getMax();

  T getMin();

  boolean countGT(T t);
  boolean countLT(T t);
  boolean countEQ(T t);

  default boolean countLTE(T t){
    return countEQ(t) || countLT(t);
  }

  default boolean countGTE(T t){
    return countEQ(t) || countGT(t);
  }

  default boolean isMin(){
    return getMin().equals(getCount());
  }

  default boolean isMax(){
    return getMax().equals(getCount());
  }

}
