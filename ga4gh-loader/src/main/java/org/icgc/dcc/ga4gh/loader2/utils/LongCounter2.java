package org.icgc.dcc.ga4gh.loader2.utils;

public class LongCounter2 implements Counter2<Long> {

  public static LongCounter2 createLongCounter2(long init) {
    return new LongCounter2(init);
  }

  private final long init;

  private long count;

  public LongCounter2(long init) {
    this.init = init;
    this.count = init;
  }

  @Override public Long incr() {
    return ++count;
  }

  @Override public Long incr(Long amount) {
    count += amount;
    return count;
  }

  @Override public void reset() {
    count = init;
  }

  @Override public Long getCount() {
    return count;
  }

  @Override public Long getMax() {
    return Long.MAX_VALUE;
  }

  @Override public Long getMin(){
    return Long.MIN_VALUE;
  }

  @Override public boolean countGT(Long longValue) {
    return count > longValue;
  }

  @Override public boolean countLT(Long longValue) {
    return count < longValue;
  }

  @Override public boolean countEQ(Long longValue) {
    return count == longValue;
  }

  @Override public boolean countLTE(Long longValue) {
    return count <= longValue;
  }

  @Override public boolean countGTE(Long longValue) {
    return count >= longValue;
  }

}
