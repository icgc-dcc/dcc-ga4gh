package org.icgc.dcc.ga4gh.loader;

import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;

public enum LoaderModes {
  AGGREGATE_ONLY(1), INDEX_ONLY_BASIC(2), FULLY_LOAD(3), INDEX_ONLY_SPECIAL(4);

  private int mode;

  private LoaderModes(final int mode) {
    this.mode = mode;
  }

  public static LoaderModes parseLoaderMode(final int inputMode) {
    boolean found = false;
    for (val loaderMode : values()) {
      if (loaderMode.getModeId() == inputMode) {
        found = true;
        return loaderMode;
      }
    }
    checkArgument(found, "The inputMode {} does not exist for LoaderModes", inputMode);
    return LoaderModes.FULLY_LOAD; // Should never be reached
  }

  public int getModeId() {
    return this.mode;
  }

}
