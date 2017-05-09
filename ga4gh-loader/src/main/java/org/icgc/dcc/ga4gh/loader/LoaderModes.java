package org.icgc.dcc.ga4gh.loader;

import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by rtisma on 2017-05-09.
 */
public enum LoaderModes {
  NESTED_ONLY(1), PARENT_CHILD_ONLY(2), PARENT_CHILD_THEN_NESTED(3);

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
    checkArgument(found, "The inputMode {} does not exist for LoaderModes2", inputMode);
    return LoaderModes.NESTED_ONLY; // Should never be reached
  }

  public int getModeId() {
    return this.mode;
  }

}
