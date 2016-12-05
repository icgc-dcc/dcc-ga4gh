package org.collaboratory.ga4gh.loader;

import static com.google.common.collect.Iterables.transform;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.icgc.dcc.common.core.json.JsonNodeBuilders.ObjectNodeBuilder;

import com.fasterxml.jackson.databind.node.ArrayNode;
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

  // TODO: [rtisma] temp
  private static final Set<String> variantIdCache = new HashSet<String>();

  public VCF(@NonNull final File file, @NonNull final FileMetaData fileMetaData) {
    this.vcf = new VCFFileReader(file, REQUIRE_INDEX_CFG);
    this.fileMetaData = fileMetaData;
    this.encoder = new VCFEncoder(vcf.getFileHeader(),
        ALLOW_MISSING_FIELDS_IN_HEADER_CFG,
        OUTPUT_TRAILING_FORMAT_FIELDS_CFG);
  }

  public ObjectNode readCallSets() {
    return convertCallSet(this.fileMetaData.getSampleId());
  }

  public Map<String, ObjectNode> readCalls() {
    val map = new HashMap<String, ObjectNode>();
    for (val record : vcf) {
      map.put(createVariantId(record), convertCallNodeObj(record));
    }
    return map;
  }

  public ObjectNode readVariantSets() {
    return convertVariantSetNodeObj(this.getHeader());
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

  private ObjectNode convertCallSetNested(String bio_sample_id, String caller_id) {
    return object()
        .with("id", createCallSetId(bio_sample_id))
        .with("name", bio_sample_id + "_" + caller_id)
        .end();
  }

  private ObjectNode convertCallSet(final String caller_id) {

    return object()
        .with("id", createCallSetId(this.fileMetaData.getSampleId()))
        .with("name", createCallSetName(this.fileMetaData.getSampleId()))
        .with("variant_set_id", createVariantSetId(caller_id))
        .with("bio_sample_id", this.fileMetaData.getSampleId())
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
      arrayObjBuilder.with(createVariantSetId(caller));
    }
    return arrayObjBuilder.end();
  }

  private ObjectNode convertCallNodeObj(@NonNull VariantContext record) {
    val parser = this.fileMetaData.getVcfFilenameParser();
    val caller_id = parser.getCallerId();
    val mutationType = parser.getMutationType();
    val mutationSubType = parser.getMutationSubType();
    val bio_sample_id = this.fileMetaData.getSampleId();

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

  @SneakyThrows
  private ObjectNode convertVariantSetNodeObj(@NonNull VCFHeader header) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    // TODO: [rtisma]: consider changing this stategy and using the raw header
    oos.writeObject(header);
    oos.close();
    val ser = Base64.getEncoder().encodeToString(baos.toByteArray());

    return object()
        .with("id", this.fileMetaData.getSampleId())
        .with("donor_id", this.fileMetaData.getDonorId())
        .with("data_set_id", this.fileMetaData.getDataType())
        .with("reference_set_id", "something")
        .with("vcf_header", ser)
        .end();
  }

  private ObjectNode convertVariantNodeObj(VariantContext record) {
    val variantId = createVariantId(record);
    variantIdCache.add(variantId);
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
