package org.icgc.dcc.ga4gh.loader.utils.counting;

import lombok.val;

public class LongCounter implements Counter<Long> {

  public static LongCounter createLongCounter(long init) {
    return new LongCounter(init);
  }

  private final long init;

  private long count;

  public LongCounter(long init) {
    this.init = init;
    this.count = init;
  }

  @Override
  public Long preIncr() {
    return ++count;
  }

  @Override
  public Long preIncr(Long amount) {
    count += amount;
    return count;
  }

  @Override
  public void reset() {
    count = init;
  }

  @Override
  public Long getCount() {
    return count;
  }

  @Override
  public Long postIncr() {
    return count++;
  }

  @Override
  public Long postIncr(Long amount) {
    val post = count;
    count += amount;
    return post;
  }
}
