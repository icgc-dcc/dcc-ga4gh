package org.icgc.dcc.ga4gh.loader.storage;

import lombok.Getter;
import lombok.NonNull;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;

public class LocalStorageFileNotFoundException extends RuntimeException{

  @Getter private final PortalMetadata portalMetadata;

  public LocalStorageFileNotFoundException(String message, @NonNull PortalMetadata portalMetadata) {
    super(message);
    this.portalMetadata = portalMetadata;
  }

}
