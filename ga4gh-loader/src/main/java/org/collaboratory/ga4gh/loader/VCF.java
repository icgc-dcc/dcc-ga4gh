package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.Closeable;
import java.io.File;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;

public class VCF implements Closeable {

  @NonNull
  private final VCFFileReader vcf;

  @NonNull
  private final FileMetaData additionalSourceData;

  @NonNull
  private final VCFEncoder encoder;

  public VCF(File file, FileMetaData additionalSourceData) {
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
    return object()
        .with("id", record.getID())
        .with("variant_set_id", "test")
        .with("start", record.getStart())
        .with("end", record.getEnd())
        .with("reference_name", record.getContig())
        .with("record", encoder.encode(record))
        .with("call_set_id", this.additionalSourceData.getObjectId())
        .with("file_id", this.additionalSourceData.getFileId())
        .with("donor_id", this.additionalSourceData.getDonorId())
        .with("sample_id", this.additionalSourceData.getSampleId())
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }

}
