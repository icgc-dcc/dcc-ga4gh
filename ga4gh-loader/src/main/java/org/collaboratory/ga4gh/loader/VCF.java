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

  public static final boolean REQUIRE_INDEX_CFG = false;
  public static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  public static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;

  @NonNull
  private final VCFFileReader vcf;

  @NonNull
  private final FileMetaData fileMetaData;

  @NonNull
  private final VCFEncoder encoder;

  public VCF(File file, FileMetaData fileMetaData) {

    this.vcf = new VCFFileReader(file, REQUIRE_INDEX_CFG);
    this.fileMetaData = fileMetaData;
    this.encoder =
        new VCFEncoder(vcf.getFileHeader(), ALLOW_MISSING_FIELDS_IN_HEADER_CFG, OUTPUT_TRAILING_FORMAT_FIELDS_CFG);
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
        .with("call_set_id", this.fileMetaData.getObjectId())
        .with("file_id", this.fileMetaData.getFileId())
        .with("donor_id", this.fileMetaData.getDonorId())
        .with("sample_id", this.fileMetaData.getSampleId())
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }

}
