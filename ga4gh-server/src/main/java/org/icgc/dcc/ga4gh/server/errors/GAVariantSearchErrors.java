package org.icgc.dcc.ga4gh.server.errors;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum GAVariantSearchErrors implements GAServerError {
  START_LESS_THAN_END("start.less.than.end", 404),
  PAGE_TOKEN_DNE("page.token.dne", 404),
  UNKNOWN("unknown", 400) ;

  @NonNull private final String id;
  private final int httpStatus;

}
