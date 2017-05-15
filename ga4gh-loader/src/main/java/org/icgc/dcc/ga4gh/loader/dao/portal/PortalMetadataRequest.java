package org.icgc.dcc.ga4gh.loader.dao.portal;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.common.model.portal.PortalFilename;

@RequiredArgsConstructor
@Data
public class PortalMetadataRequest {

  public static PortalMetadataRequest createPortalMetadataRequest(PortalFilename portalFilename) {
    return new PortalMetadataRequest(portalFilename);
  }

  @NonNull private final PortalFilename portalFilename;

}
