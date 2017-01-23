package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.DONOR_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VCF_HEADER;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Set;
import java.util.stream.Stream;

import org.collaboratory.ga4gh.loader.enums.CallerTypes;
import org.collaboratory.ga4gh.loader.enums.MutationTypes;
import org.collaboratory.ga4gh.loader.enums.SubMutationTypes;
import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsCallSet;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.model.es.EsVariantCallPair;
import org.collaboratory.ga4gh.loader.model.es.EsVariantSet;
import org.collaboratory.ga4gh.loader.model.metadata.FileMetaData;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.stream.Streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.CommonInfo;
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
  private static final double DEFAULT_GENOTYPE_LIKELYHOOD = 0.1;

  private final VCFFileReader vcf;

  private final FileMetaData fileMetaData;

  private final VCFEncoder encoder;

  private final Set<String> variantIdSet;

  public VCF(@NonNull final File file,
      @NonNull final FileMetaData fileMetaData) {
    this.vcf = new VCFFileReader(file,
        REQUIRE_INDEX_CFG);
    this.fileMetaData = fileMetaData;
    this.encoder = new VCFEncoder(vcf.getFileHeader(),

        ALLOW_MISSING_FIELDS_IN_HEADER_CFG,

        OUTPUT_TRAILING_FORMAT_FIELDS_CFG);
    this.variantIdSet = newHashSet();
  }

  public EsCallSet readCallSets() {
    return convertCallSet(fileMetaData);
  }

  private boolean isDuplicateVariantId(final VariantContext vc) {
    val variantId = createVariantId(vc);
    val duplicateVariantId = variantIdSet.contains(variantId);
    if (duplicateVariantId) {
      log.error("Detected duplicate variantId entry  [{}]. Ignoring it and moving on", variantId);
    } else {
      variantIdSet.add(variantId);
    }
    return duplicateVariantId;
  }

  // TODO: [rtisma] -- handle case where variantId already exists. If variantId exists,
  // then its corrupted as thats not allowed
  public Stream<EsCall> streamCalls() {
    return Streams.stream(vcf.iterator())
        .filter(v -> !isDuplicateVariantId(v))
        .map(v -> convertCallNodeObj(v));
  }

  public Stream<EsVariantCallPair> readVariantAndCalls() {
    return Streams.stream(vcf.iterator())
        .map(this::convertVariantCallNodeObj);
  }

  public Iterable<EsVariant> readVariants() {
    return transform(vcf,
        VCF::convertVariantNodeObj);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  // ASSUMPTION: the CallSetName is a unique string
  private static String createCallSetName(final FileMetaData fileMetaData) {
    return fileMetaData.getSampleId();
  }

  private static EsCallSet convertCallSet(final FileMetaData fileMetaData) {
    val name = createCallSetName(fileMetaData);
    return EsCallSet.builder()
        .name(name)
        .variantSetId(createVariantSetIds(fileMetaData.getVcfFilenameParser().getCallerId()))
        .bioSampleId(name) // bio_sample_id == call_set_name
        .build();
  }

  private static String createVariantId(VariantContext record) {
    return createVariantName(record); // TODO: [rtisma] temporary untill get UUID5 working
  }

  // ASSUMPTION: the VariantName is a unique string
  private static String createVariantName(VariantContext record) {
    return Joiners.UNDERSCORE.join(
        record.getStart(),
        record.getEnd(),
        record.getContig(),
        record.getReference().getBaseString(),
        Joiners.COMMA.join(record.getAlternateAlleles()));
  }

  // TODO: [rtisma] -- temporarily using until implement uuid
  private static String createVariantSetIds(String caller_id) {
    return createVariantSetName(caller_id);
  }

  private static String createVariantSetName(String caller_id) {
    return caller_id;
  }

  private boolean isMutationTypesCorrect() {
    val mutationTypeString = fileMetaData.getVcfFilenameParser().getMutationType();
    val mutationSubTypeString = fileMetaData.getVcfFilenameParser().getSubMutationType();
    return MutationTypes.somatic.equals(mutationTypeString) && (SubMutationTypes.indel.equals(mutationSubTypeString)
        || SubMutationTypes.snv_mnv.equals(mutationSubTypeString));
  }

  // TODO: [rtisma] - this method is wayyyyyyyyyyyy to big and doing to many things. refactoring needed
  private EsCall convertCallNodeObj(@NonNull VariantContext record) {
    val parser = fileMetaData.getVcfFilenameParser();
    val callerTypeString = parser.getCallerId();
    val mutationTypeString = parser.getMutationType();
    val mutationSubTypeString = parser.getSubMutationType();
    val genotypeContext = record.getGenotypes();
    val commonInfo = record.getCommonInfo();
    val parentVariant = convertVariantNodeObj(record);

    val errorMessage = "CallerType: {} not implemented";
    String tumorKey;
    boolean hasCalls = true;
    boolean foundCallerTypes = true;
    boolean foundMutationTypes = isMutationTypesCorrect();

    // TODO: [rtisma] inneficient and redundant. Find the caller type for the file at the beginning, and then just use
    // that in the if statement
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

    checkState(foundCallerTypes, "Error: the caller_id [%s] is not recognzed for filename [%s]",
        callerTypeString,
        parser.getFilename());
    checkState(foundMutationTypes,
        "Error: the mutationType(%s) must be of type [%s], and the subMutationType(%s) can be eitheror of [%s ,%s]",
        mutationTypeString,
        MutationTypes.somatic,
        mutationSubTypeString,
        SubMutationTypes.indel,
        SubMutationTypes.snv_mnv);

    val refAllele = record.getReference();
    val altAlleles = record.getAlleles();
    val alleles = newArrayList(altAlleles);
    alleles.add(refAllele);

    val numGenotypes = genotypeContext.size();

    // TODO: [rtisma] temporary untill fix above. Take first call, but not correct. Should descriminate by Sample Name
    // which should be tumorKey. Assumption is the there is ATMOST 2 calls per variant
    if (hasCalls) {
      checkState(numGenotypes > 0, "The variant [%s] should have calls for fileMetaData [%s]", record, fileMetaData);
      try {
        for (val genotype : genotypeContext) {
          // if (genotype.getSampleName().equals(tumorKey)) {
          return createCallObjectNode(fileMetaData, parentVariant, commonInfo, genotype);
          // }
        }
        return createCallObjectNode(fileMetaData, parentVariant, commonInfo, createDefaultGenotype(refAllele));
      } catch (TribbleException e) {
        if (!fileMetaData.isCorrupted()) {
          log.error("CORRUPTED VCF FILE [{}] -- Message [{}]: {}",
              fileMetaData.getVcfFilenameParser().getFilename(),
              e.getClass().getName(),
              e.getMessage());
          fileMetaData.setCorrupted(true); // Set to corrupted state so that dont have to log again
        }
        return createCallObjectNode(fileMetaData, parentVariant, commonInfo, createDefaultGenotype(refAllele));
      }
    } else {
      return createCallObjectNode(fileMetaData, parentVariant, commonInfo, createDefaultGenotype(refAllele));
    }
    // checkState(!hasCalls, "The variant [%s] should not have any calls.\nfileMetaData: [%s]",
    // record, fileMetaData);

  }

  private static ObjectNode convertInfo(@NonNull final CommonInfo info) {
    val obj = object();
    info.getAttributes().entrySet().stream()
        .forEach(e -> obj.with(e.getKey(), e.getValue().toString())); // TODO: [rtisma] -- ensure value is not
                                                                      // collection or array. If is, then need further
                                                                      // implmentation
    return obj.end();
  }

  private static EsCall createCallObjectNode(@NonNull final FileMetaData fileMetaData,
      @NonNull EsVariant parentVariant,
      @NonNull CommonInfo info,
      @NonNull Genotype genotype) {
    val parser = fileMetaData.getVcfFilenameParser();
    val callerTypeString = parser.getCallerId();
    val bioSampleId = fileMetaData.getSampleId();

    return EsCall.builder()
        .parentVariant(parentVariant) // TODO: [OPTIMIZE] once merge call and variant indexing, this
                                      // will be non-redundant
        .variantSetId(callerTypeString)
        .callSetId(bioSampleId)
        .info(info)
        .genotype(genotype)
        .build();

    // return object()
    // .with(ID, createCallName(record, callerTypeString, bioSampleId, genotype))
    // .with(NAME, createCallName(record, callerTypeString, bioSampleId, genotype))
    // .with(VARIANT_SET_ID, callerTypeString)
    // .with(CALL_SET_ID, bioSampleId)
    // .with(BIO_SAMPLE_ID, bioSampleId)
    // .with(INFO, info)
    // .with(GENOTYPE, genotypeSer)
    // .end();
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

  public EsVariantSet readVariantSet() {
    return EsVariantSet.builder()
        .name(createVariantSetName(fileMetaData.getVcfFilenameParser().getCallerId()))
        .dataSetId(fileMetaData.getDataType())
        .referenceSetId(fileMetaData.getReferenceName())
        .build();
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
        .with(VARIANT_SET_ID, createVariantSetIds(fileMetaData.getVcfFilenameParser().getCallerId()))
        .end();
  }

  private EsVariantCallPair convertVariantCallNodeObj(@NonNull final VariantContext record) {
    val call = convertCallNodeObj(record);
    val variant = EsVariant.builder()
        .start(record.getStart())
        .end(record.getEnd())
        .referenceName(record.getContig())
        .referenceBases(record.getReference().getBaseString())
        .alternativeBases(
            record.getAlternateAlleles()
                .stream()
                .map(a -> a.getBaseString())
                .collect(toImmutableList()))
        .build();
    return EsVariantCallPair.builder()
        .call(call)
        .variant(variant)
        .build();
  }

  private static EsVariant convertVariantNodeObj(@NonNull final VariantContext record) {
    return EsVariant.builder()
        .start(record.getStart())
        .end(record.getEnd())
        .referenceName(record.getContig())
        .referenceBases(record.getReference().getBaseString())
        .alternativeBases(
            record.getAlternateAlleles()
                .stream()
                .map(a -> a.getBaseString())
                .collect(toImmutableList()))
        .build();
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
