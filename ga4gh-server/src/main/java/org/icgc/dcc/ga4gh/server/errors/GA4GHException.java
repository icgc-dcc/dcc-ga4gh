package org.icgc.dcc.ga4gh.server.errors;

/**
 * Base class for all ga4gh API exceptions
 */
public class GA4GHException extends RuntimeException{

  public GA4GHException() {
    super();
  }

  public GA4GHException(String message) {
    super(message);
  }

  public GA4GHException(String message, Throwable cause) {
    super(message, cause);
  }
}
