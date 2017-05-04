package org.icgc.dcc.ga4gh.loader2.utils;

public interface Countable2<N> {

  boolean isMin();
  boolean isMax();
  N incr();
  N getCount();

}
