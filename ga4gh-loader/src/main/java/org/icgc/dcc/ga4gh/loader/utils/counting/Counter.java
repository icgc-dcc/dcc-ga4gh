package org.icgc.dcc.ga4gh.loader.utils.counting;

public interface Counter<N> {

  N incr();
  N incr(N amount);
  void reset();
  N getCount();
}
