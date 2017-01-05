package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.CALL_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.DATA_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.DONOR_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.END;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.GENOTYPE;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.INFO;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.RECORD;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.START;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VCF_HEADER;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;

import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;
import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VCF implements Closeable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;
  private static final Base64.Encoder ENCODER = Base64.getEncoder();
  private static final double DEFAULT_GENOTYPE_LIKELYHOOD = 0.1;

  private final VCFFileReader vcf;

  private final FileMetaData fileMetaData;

  private final VCFEncoder encoder;

  public VCF(@NonNull final File file,
      @NonNull final FileMetaData fileMetaData) {
    this.vcf = new VCFFileReader(file,
        REQUIRE_INDEX_CFG);
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
      map.put(createVariantId(record),
          convertCallNodeObj(record));
    }
    return map.build();
  }

  public Iterable<ObjectNode> readVariants() {
    return transform(vcf,
        this::convertVariantNodeObj);
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
      final String bio_sample_id, @NonNull final Genotype genotype) {
    return String.format("%s:%s:%s:%s", caller_id, bio_sample_id, createVariantId(record), genotype.getSampleName());
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

  @SneakyThrows
  private static String base64Serialize(@NonNull final Object o) {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    oos.writeObject(o);
    oos.close();
    return ENCODER.encodeToString(baos.toByteArray());
  }

  // TODO: [rtisma] - still need to properly implement
  private ObjectNode convertCallNodeObj(@NonNull VariantContext record) {
    val parser = fileMetaData.getVcfFilenameParser();
    val callerTypeString = parser.getCallerId();
    val mutationTypeString = parser.getMutationType();
    val mutationSubTypeString = parser.getMutationSubType();
    val genotypeContext = record.getGenotypes();
    val commonInfoSer = base64Serialize(record.getCommonInfo());

    val errorMessage = "CallerType: {} not implemented";
    String tumorKey;
    boolean hasCalls = true;
    boolean foundCallerTypes = true;
    boolean foundMutationTypes = true;

    if (MutationTypes.somatic.equals(mutationTypeString) && (SubMutationTypes.indel.equals(mutationSubTypeString)
        || SubMutationTypes.snv_mnv.equals(mutationSubTypeString))) {

      if (CallerTypes.broad.isIn(callerTypeString)) {
        tumorKey = fileMetaData.getVcfFilenameParser().getObjectId() + "T";
      } else if (CallerTypes.MUSE.isIn(callerTypeString)) {
        val objectId = fileMetaData.getVcfFilenameParser().getObjectId();
        val sampleNameSet = genotypeContext.getSampleNames();
        val numSamples = sampleNameSet.size();
        if (numSamples > 2 || numSamples == 0) {
          log.error("Incorrectly formatted VCF file for {}", fileMetaData.getVcfFilenameParser().getFilename());
        } else if (numSamples == 2) {
          for (val name : sampleNameSet) {
            if (!name.equals(objectId)) {
              tumorKey = name;
              break;
            }
          }
        } else {
          tumorKey = sampleNameSet.iterator().next();
        }

      } else if (CallerTypes.consensus.isIn(callerTypeString)) {
        hasCalls = false;
      } else if (CallerTypes.embl.isIn(callerTypeString)) {
        // log.error(errorMessage, CallerTypes.embl);
        hasCalls = false;
      } else if (CallerTypes.dkfz.isIn(callerTypeString)) {
        hasCalls = false;
      } else if (CallerTypes.svcp.isIn(callerTypeString)) {
        hasCalls = false;
      } else if (CallerTypes.svfix.isIn(callerTypeString)) {
        hasCalls = false;
      } else {
        foundCallerTypes = false;
      }
    } else {
      foundMutationTypes = false;
    }

    checkState(foundCallerTypes, "Error: the caller_id [%s] is not recognzed for filename [%s]", callerTypeString,
        parser.getFilename());
    checkState(foundMutationTypes,
        "Error: the mutationType [%s] must be of type [%s], and the subMutationType can be eitheror of [%s ,%s]",
        mutationTypeString, MutationTypes.somatic, SubMutationTypes.indel, SubMutationTypes.snv_mnv);

    val refAllele = record.getReference();
    val altAlleles = record.getAlleles();
    val alleles = newArrayList(altAlleles);
    alleles.add(refAllele);

    val numGenotypes = genotypeContext.size();

    // TODO: [rtisma] temporary untill fix above. Take first call, but not correct. Should descriminate by Sample Name
    // which should be tumorKey
    if (hasCalls) {
      checkState(numGenotypes > 0, "The variant [%s] should have calls for fileMetaData [%s]", record, fileMetaData);
      try {
        for (val genotype : genotypeContext) {
          // if (genotype.getSampleName().equals(tumorKey)) {
          return createCallObjectNode(fileMetaData, record, commonInfoSer, genotype);
          // }
        }
        return createCallObjectNode(fileMetaData, record, commonInfoSer, createDefaultGenotype(refAllele));
      } catch (TribbleException e) {
        if (!fileMetaData.isCorrupted()) {
          log.error("CORRUPTED VCF [{}] -- Message [{}]: {}",
              fileMetaData.getVcfFilenameParser().getFilename(),
              e.getClass().getName(),
              e.getMessage());
          fileMetaData.setCorrupted(true);
        }
        return createCallObjectNode(fileMetaData, record, commonInfoSer, createDefaultGenotype(refAllele));
      }
    } else {
      return createCallObjectNode(fileMetaData, record, commonInfoSer, createDefaultGenotype(refAllele));
    }
    // checkState(!hasCalls, "The variant [%s] should not have any calls.\nfileMetaData: [%s]",
    // record, fileMetaData);

  }

  private static ObjectNode createCallObjectNode(@NonNull final FileMetaData fileMetaData,
      @NonNull VariantContext record,
      @NonNull String info,
      @NonNull Genotype genotype) {
    val parser = fileMetaData.getVcfFilenameParser();
    val callerTypeString = parser.getCallerId();
    val bioSampleId = fileMetaData.getSampleId();

    val genotypeSer = base64Serialize(genotype);
    return object()
        .with(ID, createCallName(record, callerTypeString, bioSampleId, genotype))
        .with(NAME, createCallName(record, callerTypeString, bioSampleId, genotype))
        .with(VARIANT_SET_ID, callerTypeString)
        .with(CALL_SET_ID, bioSampleId)
        .with(BIO_SAMPLE_ID, bioSampleId)
        .with(INFO, info)
        .with(GENOTYPE, genotypeSer)
        .end();
  }

  private static Genotype createDefaultGenotype(@NonNull final Allele refAllele) {
    return new GenotypeBuilder("DEFAULT")
        .noAD()
        .noAttributes()
        .noDP()
        .noGQ()
        .noPL()
        .alleles(ImmutableList.of(refAllele))
        .log10PError(DEFAULT_GENOTYPE_LIKELYHOOD)
        .make();
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

  private ObjectNode convertVariantNodeObj(@NonNull final VariantContext record) {
    val variantId = createVariantId(record);
    return object()
        .with(ID, variantId)
        .with(START, record.getStart())
        .with(END, record.getEnd())
        .with(REFERENCE_NAME, record.getContig())
        .with(RECORD, encoder.encode(record))
        .end();
  }

  private ObjectNode convertCalls(final VariantContext record) {
    val genotypeContext = record.getGenotypes();
    val callInfo = record.getCommonInfo();

    return null;

  }

  @Override
  public void close() {
    vcf.close();
  }
}
