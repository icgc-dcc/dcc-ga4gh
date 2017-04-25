package org.icgc.dcc.ga4gh.common.types;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum NACodeTypes {
  NOT_APPLICABLE(-888),
  DATA_VERIFIED_TO_BE_UNKNOWN(-777),
  UNEXPECTED_DATA(-777),
  CORRUPTED_DATA(-777);

  private final int code;

  public String toString(){
    return Integer.toString(code);
  }

  public int toInt(){
    return code;
  }
}
