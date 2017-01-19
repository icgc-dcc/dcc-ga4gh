package org.collaboratory.ga4gh.loader.utils;

import lombok.Getter;

public class Counter {

  private final int initVal;

  @Getter
  private int count;

  public Counter(final int initVal) {
    this.initVal = initVal;
    this.count = initVal;
  }

  public Counter() {
    this(0);
  }

  public void incr() {
    count++;
  }

  public void incr(final int amount) {
    count += amount;
  }

  public void reset() {
    count = initVal;
  }

}
