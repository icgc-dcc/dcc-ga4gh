package org.icgc.dcc.ga4gh.loader2;

import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;

public enum LoaderModes2 {
  AGGREGATE_ONLY(1), INDEX_ONLY_BASIC(2), FULLY_LOAD(3), INDEX_ONLY_SPECIAL(4);

  private int mode;

  private LoaderModes2(final int mode) {
    this.mode = mode;
  }

  public static LoaderModes2 parseLoaderMode(final int inputMode) {
    boolean found = false;
    for (val loaderMode : values()) {
      if (loaderMode.getModeId() == inputMode) {
        found = true;
        return loaderMode;
      }
    }
    checkArgument(found, "The inputMode {} does not exist for LoaderModes2", inputMode);
    return LoaderModes2.FULLY_LOAD; // Should never be reached
  }

  public int getModeId() {
    return this.mode;
  }

}
