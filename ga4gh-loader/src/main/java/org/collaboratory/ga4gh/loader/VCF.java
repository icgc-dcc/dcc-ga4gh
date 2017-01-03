package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.CALL_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.DATA_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.DONOR_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.END;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.GENOTYPE;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.PHASESET;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.RECORD;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.REFERENCE_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.START;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.resources.mappings.IndexAttributes.VCF_HEADER;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;

import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

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
    val map = ImmutableMap.<String, ObjectNode> builder();
    for (val record : vcf) {
      map.put(createVariantId(record), convertCallNodeObj(record));
    }
    return map.build();
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
        .with(ID, createCallSetId(fileMetaData.getSampleId()))
        .with(NAME, createCallSetName(fileMetaData.getSampleId()))
        .with(VARIANT_SET_IDS, createVariantSetId(caller_id))
        .with(BIO_SAMPLE_ID, fileMetaData.getSampleId())
        .end();
  }

  private static String createVariantId(VariantContext record) {
    return createVariantName(record); // TODO: [rtisma] temporary untill get UUID5 working
  }

  private static String createVariantName(VariantContext record) {
    return Joiners.UNDERSCORE.join(
        record.getStart(),
        record.getEnd(),
        record.getContig(),
        record.getReference().getBaseString(),
        Joiners.COMMA.join(record.getAlternateAlleles()));
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
      log.error("CallerType: {} not implemented", CallerTypes.BROAD.getName());
    } else if (CallerTypes.MUSE.getName().equals(caller_id)) {
      log.error("CallerType: {} not implemented", CallerTypes.MUSE.getName());
    } else if (CallerTypes.CONSENSUS.getName().equals(caller_id)) {
      return object()
          .with(ID, createCallId(record, caller_id, bio_sample_id))
          .with(NAME, createCallName(record, caller_id, bio_sample_id))
          .with(GENOTYPE, 1)
          .with(PHASESET, "false")
          .with(VARIANT_SET_ID, caller_id)
          .with(CALL_SET_ID, bio_sample_id)
          .with(BIO_SAMPLE_ID, bio_sample_id)
          .end();

    } else if (CallerTypes.EMBL.getName().equals(caller_id)) {
      log.error("CallerType: {} not implemented", CallerTypes.EMBL.getName());
    } else if (CallerTypes.DKFZ.getName().equals(caller_id)) {
      log.error("CallerType: {} not implemented", CallerTypes.DKFZ.getName());
    } else if (CallerTypes.SVCP.getName().equals(caller_id)) {
      log.error("CallerType: {} not implemented", CallerTypes.SVCP.getName());
    } else if (CallerTypes.SVFIX.getName().equals(caller_id)) {
      log.error("CallerType: {} not implemented", CallerTypes.SVFIX.getName());
    } else {
      throw new IllegalStateException(String.format("Error: the caller_id [%s] is not recognzed for filename [%s]",
          caller_id, parser.getFilename()));
    }
    return null;// TODO: [rtisma] - fix this, cannot return null
  }

  public ObjectNode readVariantSet() {

    return object()
        .with(ID, createVariantSetId(fileMetaData.getVcfFilenameParser().getCallerId()))
        .with(NAME, createVariantSetName(fileMetaData.getVcfFilenameParser().getCallerId()))
        .with(DATA_SET_ID, fileMetaData.getDataType())
        .with(REFERENCE_SET_ID, fileMetaData.getReferenceName())
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
        .with(VCF_HEADER, ser)
        .with(DONOR_ID, fileMetaData.getDonorId())
        .with(BIO_SAMPLE_ID, fileMetaData.getSampleId())
        .with(VARIANT_SET_ID, createVariantSetId(fileMetaData.getVcfFilenameParser().getCallerId()))
        .end();
  }

  private ObjectNode convertVariantNodeObj(VariantContext record) {
    val variantId = createVariantId(record);
    return object()
        .with(ID, variantId)
        .with(START, record.getStart())
        .with(END, record.getEnd())
        .with(REFERENCE_NAME, record.getContig())
        .with(RECORD, encoder.encode(record))
        .end();
  }

  @Override
  public void close() {
    vcf.close();
  }
}
