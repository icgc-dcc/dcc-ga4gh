package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.Closeable;
import java.io.File;
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

  public Iterable<ObjectNode> read() {
    return transform(vcf, this::convert);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private ObjectNode convert(VariantContext record) {
    val jsonArrayNode = array().with(
        record.getCommonInfo().getAttributeAsList("Callers")
            .stream()
            .map(x -> x.toString())
            .collect(Collectors.toList()));

    return object()
        .with("id", record.getID())
        .with("start", record.getStart())
        .with("end", record.getEnd())
        .with("reference_name", record.getContig())
        .with("record", encoder.encode(record))
        .with("call_set_id", this.fileMetaData.getFileId())
        .with("variant_set_id", this.fileMetaData.getSampleId())
        .with("donor_id", this.fileMetaData.getDonorId())
        .with("data_type", this.fileMetaData.getDataType())
        .with("bio_sample_id", this.fileMetaData.getSampleId())
        .with("caller_ids", jsonArrayNode)
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }

}
