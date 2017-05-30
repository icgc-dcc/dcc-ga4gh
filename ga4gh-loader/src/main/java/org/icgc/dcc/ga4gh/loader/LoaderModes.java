package org.icgc.dcc.ga4gh.loader;

import lombok.val;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;

public enum LoaderModes {
  AGGREGATE_ONLY(1), INDEX_ONLY_BASIC(2), FULLY_LOAD(3), INDEX_ONLY_SPECIAL(4);

  private int mode;

  private LoaderModes(final int mode) {
    this.mode = mode;
  }

  public int getModeId() {
    return this.mode;
  }

  public static LoaderModes parseLoaderMode(final int inputMode) {
    val mode = stream(values()).filter(l -> l.getModeId()==inputMode).findFirst();
    return mode.orElseThrow(() -> new IllegalArgumentException(String.format("The inputMode {} does not exist for LoaderModes", inputMode)));
  }

}
