package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import org.icgc.dcc.common.core.json.JsonNodeBuilders.ObjectNodeBuilder;

import com.fasterxml.jackson.databind.node.ArrayNode;
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
    return transform(vcf, this::convertVariantNodeObj);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private static String createCallSetId(String bio_sample_id, String caller_id) {
    return createCallSetName(bio_sample_id, caller_id); // TODO: [rtisma] temporary untill get UUID5 working
  }

  private static String createCallSetName(String bio_sample_id, String caller_id) {
    return bio_sample_id + caller_id;
  }

  private ObjectNode convertCallSetNested(String bio_sample_id, String caller_id) {
    return object()
        .with("id", createCallSetId(bio_sample_id, caller_id))
        .with("name", bio_sample_id + "_" + caller_id)
        .end();
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
    return createVariantName(record); // TODO: [rtisma] temporary untill get UUID5 working
  }

  private static String createVariantName(VariantContext record) {
    return record.getStart() + "_" + record.getEnd() + "_" + record.getContig();
  }

  private static String createVariantSetId(String caller_id) {
    return caller_id;
  }

  private static ObjectNodeBuilder createNodeBuilderFromMap(@NonNull Map<String, String> map) {
    val objNode = object();
    map.entrySet().stream().forEach(entry -> objNode.with(entry.getKey(), entry.getValue()));
    return objNode;
  }

  private ArrayNode convertVariantSetArrayObj(@NonNull VariantContext record) {
    val caller_list = record.getCommonInfo().getAttributeAsList("Callers").stream().map(c -> c.toString())
        .collect(Collectors.toList());
    val arrayObjBuilder = array();
    for (String caller : caller_list) {
      arrayObjBuilder.with(
          object()
              .with("id", createVariantSetId(caller))
              .with("name", caller)
              .with("data_set_id", this.fileMetaData.getDataType())
              .with("reference_set_id", "something")
              .end());
    }
    return arrayObjBuilder.end();
  }

  private ArrayNode convertCallArrayNodeObj(@NonNull VariantContext record) {
    val caller_list = record.getCommonInfo().getAttributeAsList("Callers").stream().map(c -> c.toString())
        .collect(Collectors.toList());
    val arrayObjBuilder = array();
    for (String caller : caller_list) {
      arrayObjBuilder.with(
          object()
              .with("name", createVariantName(record) + ":" + this.fileMetaData.getSampleId() + "_" + caller)
              .with("call_set_id", createCallSetId(this.fileMetaData.getSampleId(), caller))
              .with("call_set_name", createCallSetName(this.fileMetaData.getSampleId(), caller))
              .with("phaseset", "false")
              .with("genotype", 1)
              .end());
    }
    return arrayObjBuilder.end();
  }

  private ObjectNode convertVariantNodeObj(VariantContext record) {

    return object()
        .with("id", createVariantId(record))
        .with("start", record.getStart())
        .with("end", record.getEnd())
        .with("reference_name", record.getContig())
        .with("record", encoder.encode(record))
        .with("variant_set", convertVariantSetArrayObj(record))
        .with("calls", convertCallArrayNodeObj(record))
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
