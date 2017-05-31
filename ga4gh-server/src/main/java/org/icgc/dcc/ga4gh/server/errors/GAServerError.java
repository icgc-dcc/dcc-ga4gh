package org.icgc.dcc.ga4gh.server.errors;

import static java.lang.String.format;

/**
 * The sumo API error interface for an extensible enum pattern.
*/
public interface GAServerError {

  /**
   * Returns the identifier of the error., e.g.,
   * "page.token.dne" when the page token no longer exists
   *
   * @return The identifier of the error
   */

  String getId();

  /**
   * Returns the http status code
   * @return http status code
   */
  int getHttpStatus();

  default String createPrefixedMessage(String message, Object ... args){
    return format("["+getId()+"]: "+message, args);
  }

}
