package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.io.Closeable;
import java.io.File;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.val;

public class VCF implements Closeable {

  @NonNull
  private final VCFFileReader vcf;

  @NonNull
  private final AdditionalSourceData additionalSourceData;

  @NonNull
  private final VCFEncoder encoder;

  public VCF(File file, AdditionalSourceData additionalSourceData) {
    this.vcf = new VCFFileReader(file, false);
    this.additionalSourceData = additionalSourceData;
    this.encoder = new VCFEncoder(vcf.getFileHeader(), true, true);
  }

  public Iterable<ObjectNode> read() {
    return transform(vcf, this::convert);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private ObjectNode convert(VariantContext record) {
    val variantDoc = DEFAULT.createObjectNode();
    variantDoc.put("id", record.getID());
    variantDoc.put("variant_set_id", "test");
    variantDoc.put("start", record.getStart());
    variantDoc.put("end", record.getEnd());
    variantDoc.put("reference_name", record.getContig());
    variantDoc.put("record", encoder.encode(record));
    variantDoc.put("call_set_id", this.additionalSourceData.getObjectId());
    variantDoc.put("file_id", this.additionalSourceData.getFileId());
    variantDoc.put("donor_id", this.additionalSourceData.getDonorId());
    variantDoc.put("sample_id", this.additionalSourceData.getSampleId());

    return variantDoc;
  }

  @Override
  public void close() {
    vcf.close();
  }

}
