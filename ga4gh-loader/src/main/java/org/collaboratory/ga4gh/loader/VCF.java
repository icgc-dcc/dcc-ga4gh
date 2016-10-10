package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.Closeable;
import java.io.File;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;

public class VCF implements Closeable {

  @NonNull
  private final VCFFileReader vcf;

  public VCF(File file) {
    this.vcf = new VCFFileReader(file, false);
  }

  public Iterable<ObjectNode> read() {
    return transform(vcf, this::convert);
  }

  private ObjectNode convert(VariantContext record) {
    return DEFAULT.convertValue(record, ObjectNode.class);
  }

  @Override
  public void close() {
    vcf.close();
  }

}
