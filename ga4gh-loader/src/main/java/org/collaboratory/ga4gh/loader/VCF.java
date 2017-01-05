package org.collaboratory.ga4gh.loader;

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

import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

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

enum SubMutationTypes {
  cnv, indel, snv_mnv, sv;

  public boolean equals(@NonNull final String name) {
    return name().equals(name);
  }

  @Override
  public String toString() {
    return name();
  }
}

enum MutationTypes {
  somatic, germline;

  public boolean equals(@NonNull final String name) {
    return name().equals(name);
  }

  @Override
  public String toString() {
    return name();
  }
}

enum CallerTypes {
  consensus, MUSE, dkfz, embl, svfix, svcp, broad;

  public boolean equals(@NonNull final String name) {
    return name().equals(name);
  }

  public boolean isIn(@NonNull final String name) {
    return name.matches("^" + name() + ".*");
  }

  @Override
  public String toString() {
    return name();
  }
}

@Slf4j
public class VCF implements Closeable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;
  private static final Base64.Encoder ENCODER = Base64.getEncoder();

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
    for (val v : vcf) {
      log.info("sdfsdf");
      for (val c : v.getGenotypes()) {
        val name = c.getSampleName();
        val map = c.getExtendedAttributes();
        val alleles = c.getAlleles();
        val dp = c.getDP();
        val filters = c.getFilters();
        val gtString = c.getGenotypeString();
        val gq = c.getGQ();
        val lh = c.getLikelihoods();
        val error = c.getLog10PError();
        val pl = c.getPL();
        val ploidy = c.getPloidy();
        val ad = c.getAD();
        log.info("sdfsdf2");

      }
    }
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
    val caller_id = parser.getCallerId();
    val mutationType = parser.getMutationType();
    val mutationSubType = parser.getMutationSubType();
    val bio_sample_id = fileMetaData.getSampleId();
    val genotypeContext = record.getGenotypes();
    val commonInfoSer = base64Serialize(record.getCommonInfo());

    val errorMessage = "CallerType: {} not implemented";
    String tumorKey;
    boolean hasNoCalls = false;

    if (MutationTypes.somatic.equals(mutationType) && (SubMutationTypes.indel.equals(mutationSubType)
        || SubMutationTypes.snv_mnv.equals(mutationSubType))) {

      if (CallerTypes.broad.isIn(caller_id)) {
        tumorKey = fileMetaData.getVcfFilenameParser().getObjectId() + "T";
      } else if (CallerTypes.MUSE.isIn(caller_id)) {
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

      } else if (CallerTypes.consensus.isIn(caller_id)) {
        hasNoCalls = true;
      } else if (CallerTypes.embl.isIn(caller_id)) {
        // log.error(errorMessage, CallerTypes.embl);
        hasNoCalls = true;
      } else if (CallerTypes.dkfz.isIn(caller_id)) {
        hasNoCalls = true;
      } else if (CallerTypes.svcp.isIn(caller_id)) {
        hasNoCalls = true;
      } else if (CallerTypes.svfix.isIn(caller_id)) {
        hasNoCalls = true;
      } else {
        throw new IllegalStateException(String.format("Error: the caller_id [%s] is not recognzed for filename [%s]",
            caller_id, parser.getFilename()));
      }
    } else {
      throw new IllegalStateException(String.format(
          "Error: the mutationType [%s] must be of type [%s], and the subMutationType can be eitheror of [%s ,%s]",
          mutationType, MutationTypes.somatic, SubMutationTypes.indel, SubMutationTypes.snv_mnv));
    }
    // TODO: [rtisma] temporary untill fix above. Take first call, but not correct. Should descriminate by Sample Name
    // which should be tumorKey
    for (val genotype : genotypeContext) {
      // if (genotype.getSampleName().equals(tumorKey)) {
      val genotypeSer = base64Serialize(genotype);
      return object()
          .with(NAME, createCallName(record, caller_id, bio_sample_id, genotype))
          .with(VARIANT_SET_ID, caller_id)
          .with(CALL_SET_ID, bio_sample_id)
          .with(BIO_SAMPLE_ID, bio_sample_id)
          .with(INFO, commonInfoSer)
          .with(GENOTYPE, genotypeSer)
          .end();

      // }
    }
    val refAllele = record.getReference();
    val altAlleles = record.getAlleles();
    val alleles = newArrayList(altAlleles);
    alleles.add(refAllele);

    val genotype = new GenotypeBuilder("DEFAULT", alleles)
        .noAD()
        .noDP()
        .noPL()
        .noGQ()
        .phased(false)
        .noAttributes()
        .make();
    val genotypeSer = base64Serialize(genotype);
    if (hasNoCalls) {
      return object()
          .with(NAME, createCallName(record, caller_id, bio_sample_id, genotype))
          .with(VARIANT_SET_ID, caller_id)
          .with(CALL_SET_ID, bio_sample_id)
          .with(BIO_SAMPLE_ID, bio_sample_id)
          .with(INFO, commonInfoSer)
          .with(GENOTYPE, genotypeSer)
          .end();
    } else {
      throw new IllegalStateException(
          String.format("no object was created for \nfileMetaData: [%s]\nvariantContext: [%s]", fileMetaData, record));
    }

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
    val callName = createCallName(record);

    return null;

  }

  @Override
  public void close() {
    vcf.close();
  }
}
