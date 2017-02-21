package org.collaboratory.ga4gh.core;

import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class TypeNames {
  public static final String CALL_SET = "callset";
  public static final String CALL = "call";
  public static final String VARIANT_SET = "variant_set";
  public static final String VARIANT = "variant";
  public static final String VARIANT_NESTED = "variant_nested";
  public static final String VCF_HEADER = "vcf_header";

  public static final String getAggNameForType(final String typeName){
    return "by_"+typeName;
  }
}
