package org.icgc.dcc.ga4gh.common.model.es;

import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.ga4gh.common.model.portal.PortalMetadata;

import java.io.Serializable;

@RequiredArgsConstructor
public class EsVcfHeader implements Serializable{

  private static final long serialVersionUID = 1493648238L;

  public static EsVcfHeader createEsVcfHeader(PortalMetadata portalMetadata, VCFHeader vcfHeader) {
    return new EsVcfHeader(portalMetadata, vcfHeader);
  }

  @NonNull private final PortalMetadata portalMetadata;
  @NonNull private final VCFHeader vcfHeader;

}
