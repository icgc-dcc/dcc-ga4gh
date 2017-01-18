package org.collaboratory.ga4gh.loader.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Counter {

  private int count;

  public Counter() {
    this(0);
  }

  public void incr() {
    count++;
  }

  public void incr(final int amount) {
    count += amount;
  }

}
