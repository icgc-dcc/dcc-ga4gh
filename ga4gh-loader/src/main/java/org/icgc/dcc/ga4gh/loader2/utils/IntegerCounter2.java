package org.icgc.dcc.ga4gh.loader2.utils;

public class IntegerCounter2 implements Counter2<Integer> {

  public static IntegerCounter2 createIntegerCounter2(int init) {
    return new IntegerCounter2(init);
  }

  private final int init;

  private int count;

  public IntegerCounter2(int init) {
    this.init = init;
    this.count = init;
  }

  @Override public Integer incr() {
    return ++count;
  }

  @Override public Integer incr(Integer amount) {
    count += amount;
    return count;
  }

  @Override public void reset() {
    count = init;
  }

  @Override public Integer getCount() {
    return count;
  }

  @Override public Integer getMax() {
    return Integer.MAX_VALUE;
  }

  @Override public Integer getMin(){
    return Integer.MIN_VALUE;
  }

  @Override public boolean countGT(Integer integer) {
    return count > integer;
  }

  @Override public boolean countLT(Integer integer) {
    return count < integer;
  }

  @Override public boolean countEQ(Integer integer) {
    return count == integer;
  }

  @Override public boolean countLTE(Integer integer) {
    return count <= integer;
  }

  @Override public boolean countGTE(Integer integer) {
    return count >= integer;
  }

}
