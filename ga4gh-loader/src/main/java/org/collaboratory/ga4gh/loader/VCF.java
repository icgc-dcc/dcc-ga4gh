package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import com.fasterxml.jackson.databind.node.ObjectNode;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Getter
enum SubMutationTypes {
  CNV("cnv"), INDEL("indel"), SNV_MNV("snv_mnv"), SV("sv");

  @NonNull
  private final String name;
}

@RequiredArgsConstructor
@Getter
enum MutationTypes {
  SOMATIC("somatic"), GERMLINE("germline");

  @NonNull
  private final String name;
}

@RequiredArgsConstructor
@Getter
enum CallerTypes {
  CONSENSUS("consensus"), MUSE("MUSE"), DKFZ("dkfz"), EMBL("embl"), SVFIX("svfix"), SVCP("svcp"), BROAD("broad");

  @NonNull
  private final String name;
}

@Slf4j
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

  public ObjectNode readCallSets() {
    return convertCallSet(fileMetaData.getVcfFilenameParser().getCallerId());
  }

  public Map<String, ObjectNode> readCalls() {
    val map = new HashMap<String, ObjectNode>();
    for (val record : vcf) {
      map.put(createVariantId(record), convertCallNodeObj(record));
    }
    return map;
  }

  public Iterable<ObjectNode> readVariants() {
    return transform(vcf, this::convertVariantNodeObj);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private static String createCallSetId(String bio_sample_id) {
    return createCallSetName(bio_sample_id); // TODO: [rtisma] temporary untill get UUID5 working
  }

  private static String createCallSetName(String bio_sample_id) {
    return bio_sample_id;
  }

  private static String createCallName(@NonNull final VariantContext record, final String caller_id,
      final String bio_sample_id) {
    return createVariantId(record) + ":" + bio_sample_id + "_" + caller_id;
  }

  // TODO: [rtisma] - fix later so proper uuid
  private static String createCallId(@NonNull final VariantContext record, final String caller_id,
      final String bio_sample_id) {
    return createCallName(record, caller_id, bio_sample_id);
  }

  private ObjectNode convertCallSet(final String caller_id) {

    return object()
        .with("id", createCallSetId(fileMetaData.getSampleId()))
        .with("name", createCallSetName(fileMetaData.getSampleId()))
        .with("variant_set_ids", createVariantSetId(caller_id))
        .with("bio_sample_id", fileMetaData.getSampleId())
        .end();

  }

  private static String createVariantId(VariantContext record) {
    return createVariantName(record); // TODO: [rtisma] temporary untill get UUID5 working
  }

  private static String createName(final String delim, @NonNull final Object... array) {
    return stream(array).map(x -> x.toString()).collect(Collectors.joining(delim));
  }

  private static String createVariantName(VariantContext record) {
    return createName("_",
        record.getStart(),
        record.getEnd(),
        record.getContig(),
        record.getReference().getBaseString(),
        record.getAlternateAlleles().stream().map(al -> al.getBaseString()).collect(Collectors.joining(",")));
  }

  // TODO: [rtisma] -- temporarily using until implement uuid
  private static String createVariantSetId(String caller_id) {
    return createVariantSetName(caller_id);
  }

  private static String createVariantSetName(String caller_id) {
    return caller_id;
  }

  private ObjectNode convertCallNodeObj(@NonNull VariantContext record) {
    val parser = fileMetaData.getVcfFilenameParser();
    val caller_id = parser.getCallerId();
    val mutationType = parser.getMutationType();
    val mutationSubType = parser.getMutationSubType();
    val bio_sample_id = fileMetaData.getSampleId();

    if (CallerTypes.BROAD.getName().equals(caller_id)) {

    } else if (CallerTypes.MUSE.getName().equals(caller_id)) {
      log.error("CallerType: " + CallerTypes.MUSE.getName() + " not implemented");
    } else if (CallerTypes.CONSENSUS.getName().equals(caller_id)) {
      return object()
          .with("id", createCallId(record, caller_id, bio_sample_id))
          .with("name", createCallName(record, caller_id, bio_sample_id))
          .with("genotype", 1)
          .with("phaseset", "false")
          .with("variant_set_id", caller_id)
          .with("call_set_id", bio_sample_id)
          .with("bio_sample_id", bio_sample_id)
          .end();

    } else if (CallerTypes.EMBL.getName().equals(caller_id)) {
      log.error("CallerType: " + CallerTypes.EMBL.getName() + " not implemented");
    } else if (CallerTypes.DKFZ.getName().equals(caller_id)) {
      log.error("CallerType: " + CallerTypes.DKFZ.getName() + " not implemented");
    } else if (CallerTypes.SVCP.getName().equals(caller_id)) {
      log.error("CallerType: " + CallerTypes.SVCP.getName() + " not implemented");
    } else if (CallerTypes.SVFIX.getName().equals(caller_id)) {
      log.error("CallerType: " + CallerTypes.SVFIX.getName() + " not implemented");
    } else {
      throw new RuntimeException("Error: the caller_id: " + caller_id + " is not recognized, " + parser.getFilename());
    }
    return null;

  }

  public ObjectNode readVariantSet() {

    return object()
        .with("id", createVariantSetId(fileMetaData.getVcfFilenameParser().getCallerId()))
        .with("name", createVariantSetName(fileMetaData.getVcfFilenameParser().getCallerId()))
        .with("data_set_id", fileMetaData.getDataType())
        .with("reference_set_id", fileMetaData.getReferenceName())
        .end();
  }

  @SneakyThrows
  public ObjectNode readVCFHeader() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    // TODO: [rtisma]: consider changing this stategy and using the raw header
    oos.writeObject(getHeader());
    oos.close();
    val ser = Base64.getEncoder().encodeToString(baos.toByteArray());
    return object()
        .with("vcf_header", ser)
        .with("donor_id", fileMetaData.getDonorId())
        .with("bio_sample_id", fileMetaData.getSampleId())
        .with("variant_set_id", createVariantSetId(fileMetaData.getVcfFilenameParser().getCallerId()))
        .end();
  }

  private ObjectNode convertVariantNodeObj(VariantContext record) {
    val variantId = createVariantId(record);
    return object()
        .with("id", variantId)
        .with("start", record.getStart())
        .with("end", record.getEnd())
        .with("reference_name", record.getContig())
        .with("record", encoder.encode(record))
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }

}
