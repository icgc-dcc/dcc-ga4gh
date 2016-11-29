package org.collaboratory.ga4gh.loader;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.val;

public class VCF implements Closeable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;

  private final VCFFileReader vcf;

  private final FileMetaData fileMetaData;

  private final VCFEncoder encoder;

  public VCF(@NonNull final File file, @NonNull final FileMetaData fileMetaData) {
    this.vcf = new VCFFileReader(file, REQUIRE_INDEX_CFG);
    this.fileMetaData = fileMetaData;
    this.encoder = new VCFEncoder(vcf.getFileHeader(),
        ALLOW_MISSING_FIELDS_IN_HEADER_CFG,
        OUTPUT_TRAILING_FORMAT_FIELDS_CFG);
  }

  public Iterable<ObjectNode> readCallSets() {
    val outList = new ArrayList<ObjectNode>();
    for (VariantContext record : vcf) {
      outList.addAll(record.getCommonInfo().getAttributeAsList("Callers")
          .stream()
          .map(c -> convertCallSet(fileMetaData.getSampleId(), c.toString()))
          .collect(Collectors.toList()));
    }
    return outList;
  }

  public Iterable<ObjectNode> readVariants() {
    val outList = new ArrayList<ObjectNode>();
    for (VariantContext record : vcf) {
      outList.addAll(record.getCommonInfo().getAttributeAsList("Callers")
          .stream()
          .map(c -> convert(record, c.toString()))
          .collect(Collectors.toList()));
    }
    return outList;
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private static String createCallSetId(String bio_sample_id, String caller_id) {
    return bio_sample_id + caller_id;
  }

  private ObjectNode convertCallSet(String bio_sample_id, String caller_id) {
    return object()
        .with("id", createCallSetId(bio_sample_id, caller_id))
        .with("name", bio_sample_id + "_" + caller_id)
        .with("caller_id", caller_id)
        .with("variant_set_id", createVariantSetId(caller_id))
        .with("data_set_id", this.fileMetaData.getDataType())
        .end();

  }

  private static String createVariantId(VariantContext record) {
    return record.getStart() + "_" + record.getEnd() + "_" + record.getContig();
  }

  private static String createVariantSetId(String caller_id) {
    return caller_id;
  }

  private ObjectNode convert(VariantContext record, String caller_id) {

    return object()
        .with("id", createVariantId(record))
        .with("start", record.getStart())
        .with("end", record.getEnd())
        .with("reference_name", record.getContig())
        .with("record", encoder.encode(record))
        .with("caller_id", caller_id)
        .with("variant_set_id", createVariantSetId(caller_id))
        .with("call_set_id", createCallSetId(this.fileMetaData.getSampleId(), caller_id))
        .with("donor_id", this.fileMetaData.getDonorId())
        .with("data_type", this.fileMetaData.getDataType())
        .with("bio_sample_id", this.fileMetaData.getSampleId())
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }

}
