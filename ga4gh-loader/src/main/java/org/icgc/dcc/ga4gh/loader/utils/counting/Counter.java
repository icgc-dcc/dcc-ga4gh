package org.icgc.dcc.ga4gh.loader.utils.counting;

public interface Counter<N> {

  N preIncr();
  N preIncr(N amount);
  N postIncr();
  N postIncr(N amount);
  void reset();
  N getCount();
}
