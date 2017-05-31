package org.icgc.dcc.ga4gh.server.errors;

import ga4gh.Common.GAException;
import lombok.Getter;

@Getter
public class GAServerException extends GA4GHException{

  private final GAServerError error;

  private GAServerException(String errorMessage) {
    super(errorMessage);
    this.error = null;
  }

  private GAServerException(GAServerError error, String errorMessage) {
    super(errorMessage);
    this.error = error;
  }

  private GAServerException(GAServerError error) {
    super();
    this.error = error;
  }

  /**
   * Returns true, if the error is equal to a server error
   *
   * @param otherError The error to compare
   * @return True, if the exception equals the given server error
   */
  public boolean equals(GAServerError otherError) {
    if (otherError != null  && this.error !=null){
      return error.getId().equals(otherError.getId().toLowerCase().trim());
    } else {
      return otherError == error;
    }
  }

  public GAException createGAException(){
    return GAException.newBuilder()
        .setMessage(getMessage())
        .setErrorCode(error == null ? 400 : error.getHttpStatus())
        .build();
  }

  private static String createMessage(String message, Object ... args){
    return String.format(message, args);
  }

  private static String createIdMessage(String id, String message, Object ... args){
    return createMessage("["+id+"]: "+message, args);
  }

  private static String createGenericIdMessage(String message, Object ... args){
    return createIdMessage("generic.error", message, args);
  }

  public static GAServerException createGAServerException(GAServerError error, String errorMessage, Object ... args) {
    return new GAServerException(error, error.createPrefixedMessage(errorMessage, args));
  }

  public static GAServerException createGAServerException(String errorMessage, Object ... args) {
    return new GAServerException(createGenericIdMessage(errorMessage, args));
  }

}
