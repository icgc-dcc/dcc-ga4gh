package org.icgc.dcc.ga4gh.server.errors;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum GAVariantSearchErrors implements GAServerError {
  START_GREATER_THAN_END("start.greater.than.end", 400),
  PAGE_TOKEN_DNE("page.token.dne", 404),
  UNKNOWN("unknown", 400) ;

  @NonNull private final String id;
  private final int httpStatus;

}
