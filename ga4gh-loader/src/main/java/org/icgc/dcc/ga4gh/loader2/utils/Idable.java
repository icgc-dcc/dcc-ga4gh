package org.icgc.dcc.ga4gh.loader2.utils;

import lombok.AllArgsConstructor;

public interface Idable<N> {

  @AllArgsConstructor
  static class LongId implements Idable<Long>{

    public static LongId createLongId(long value) {
      return new LongId(value);
    }

    private long value;

    @Override public Long getId() {
      return value;
    }

    @Override public void setId(Long value) {
      this.value = value;
    }

  }

  @AllArgsConstructor
  static class IntegerId implements Idable<Integer>{

    public static IntegerId createIntegerId(int value) {
      return new IntegerId(value);
    }

    private int value;

    @Override public Integer getId() {
      return value;
    }

    @Override public void setId(Integer value) {
      this.value = value;
    }
  }

  N getId();
  void setId(N value);

}
