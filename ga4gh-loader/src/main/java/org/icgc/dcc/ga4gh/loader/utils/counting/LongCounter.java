package org.icgc.dcc.ga4gh.loader.utils.counting;

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

  public Long incr() {
    return ++count;
  }

  public Long incr(Long amount) {
    count += amount;
    return count;
  }

  public void reset() {
    count = init;
  }

  public Long getCount() {
    return count;
  }

}
