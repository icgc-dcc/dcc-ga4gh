package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Preconditions.checkState;
import static org.collaboratory.ga4gh.core.Names.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.Names.DONOR_ID;
import static org.collaboratory.ga4gh.core.Names.VARIANT_SET_ID;
import static org.collaboratory.ga4gh.core.Names.VCF_HEADER;
import static org.collaboratory.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
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
import org.collaboratory.ga4gh.loader.utils.CounterMonitor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
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
  private static final int DEFAULT_REFERENCE_ALLELE_POSITION = 0;
  private static final int ALTERNATIVE_ALLELE_INDEX_OFFSET = 1;

  private final VCFFileReader vcf;

  private final FileMetaData fileMetaData;

  private final VCFEncoder encoder;

  private final CallerTypes callerType;
  private final String actualCallerId;
  private final String sampleId;
  private final String mutationTypeString;
  private final String mutationSubTypeString;
  private final boolean isMutationTypesCorrect;
  private final String filename;

  private final CounterMonitor variantCallPairMonitor = newMonitor("VariantCallPairParsing", 250000);

  public VCF(@NonNull final File file, @NonNull final FileMetaData fileMetaData) {
    this.vcf = new VCFFileReader(file, REQUIRE_INDEX_CFG);
    this.fileMetaData = fileMetaData;
    this.encoder = new VCFEncoder(vcf.getFileHeader(),

        ALLOW_MISSING_FIELDS_IN_HEADER_CFG,

        OUTPUT_TRAILING_FORMAT_FIELDS_CFG);

    this.sampleId = fileMetaData.getSampleId();
    val parser = fileMetaData.getVcfFilenameParser();
    this.callerType = parser.getCallerType();
    this.actualCallerId = parser.getCallerId();
    this.mutationTypeString = parser.getMutationType();
    this.mutationSubTypeString = parser.getSubMutationType();
    this.isMutationTypesCorrect = isMutationTypesCorrect(mutationTypeString, mutationSubTypeString);
    this.filename = parser.getFilename();

    checkState(isMutationTypesCorrect,
        "Error: the mutationType(%s) must be of type [%s], and the subMutationType(%s) can be eitheror of [%s ,%s]",
        mutationTypeString,
        MutationTypes.somatic,
        mutationSubTypeString,
        SubMutationTypes.indel,
        SubMutationTypes.snv_mnv);

  }

  public EsCallSet readCallSets() {
    return convertCallSet(fileMetaData);
  }

  public Stream<EsVariantCallPair> readVariantAndCalls() {
    return stream(vcf.iterator())
        .map(this::convertVariantCallPair);
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

  // TODO: [rtisma] -- temporarily using until implement uuid
  private static String createVariantSetIds(String caller_id) {
    return createVariantSetName(caller_id);
  }

  private static String createVariantSetName(String caller_id) {
    return caller_id;
  }

  private static boolean isMutationTypesCorrect(String mutationTypeString, String mutationSubTypeString) {
    return MutationTypes.somatic.equals(mutationTypeString) && (SubMutationTypes.indel.equals(mutationSubTypeString)
        || SubMutationTypes.snv_mnv.equals(mutationSubTypeString));
  }

  // TODO: [rtisma] - this method is wayyyyyyyyyyyy to big and doing to many things. refactoring needed
  private List<EsCall> convertCalls(final VariantContext variantContext) {

    val errorMessage = "CallerType: {} not implemented";
    String tumorKey;
    boolean hasCalls = true;
    boolean foundCallerTypes = true;

    // TODO: [rtisma] inneficient and redundant. Find the caller type for the file at the beginning, and then just use
    // that in the if statement
    // rtisma if (callerType == CallerTypes.MUSE_1_0rc_b391201_vcf || callerType == CallerTypes.MUSE_1_0rc_vcf) {
    // rtisma val objectId = fileMetaData.getVcfFilenameParser().getObjectId();
    // rtisma val sampleNameSet = genotypeContext.getSampleNames();
    // rtisma val numSamples = sampleNameSet.size();
    // rtisma if (numSamples > 2 || numSamples == 0) {
    // rtisma log.error("Incorrectly formatted VCF file for {}", fileMetaData.getVcfFilenameParser().getFilename());
    // rtisma } else if (numSamples == 2) {
    // rtisma for (val name : sampleNameSet) {
    // rtisma if (!name.equals(objectId)) {
    // rtisma tumorKey = name;
    // rtisma break;
    // rtisma }
    // rtisma }
    // rtisma } else {
    // rtisma tumorKey = sampleNameSet.iterator().next();
    // rtisma }
    // rtisma } else {
    // rtisma foundCallerTypes = false;
    // rtisma }

    // checkState(foundCallerTypes, "Error: the callerType [%s] is not recognzed for filename [%s]",
    // callerType,
    // filename);

    val numGenotypes = variantContext.getGenotypes().size();

    // TODO: [rtisma] temporary untill fix above. Take first call, but not correct. Should descriminate by Sample Name
    // which should be tumorKey. Assumption is the there is ATMOST 2 calls per variant
    checkState(numGenotypes > 0, "The Genotypes [%s] should have calls for fileMetaData [%s]",
        variantContext.getGenotypes(),
        fileMetaData);
    boolean foundCall = false;
    // TODO: HACKKK need to properly select which call can be indexed. Calls associated with "normal" are not to be
    // indexed
    return createEsCall(actualCallerId, sampleId, variantContext);
    // rtisma try {
    // rtisma return createEsCall(actualCallerId, sampleId, genotypesContext, commonInfoMap);
    // rtisma // for (val call : callList) {
    // rtisma // // if (genotype.getSampleName().equals(tumorKey)) {
    // rtisma // return call;
    // rtisma // // }
    // rtisma // }
    // rtisma } catch (TribbleException e) {
    // rtisma if (!fileMetaData.isCorrupted()) {
    // rtisma log.error("CORRUPTED VCF FILE [{}] -- Message [{}]: {}",
    // rtisma fileMetaData.getVcfFilenameParser().getFilename(),
    // rtisma e.getClass().getName(),
    // rtisma e.getMessage());
    // rtisma fileMetaData.setCorrupted(true); // Set to corrupted state so that dont have to log again
    // rtisma }
    // rtisma throw propagate(e);
    // rtisma }
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

  private EsVariantCallPair convertVariantCallPair(final VariantContext record) {
    variantCallPairMonitor.start();
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

    val calls = convertCalls(record);

    val pair = EsVariantCallPair.builder()
        .calls(calls)
        .variant(variant)
        .build();

    variantCallPairMonitor.incr();
    variantCallPairMonitor.stop();

    return pair;
  }

  @Override
  public void close() {
    vcf.close();
  }

  /*
   * Returns a list contain Allele IDs associated with the maternal and paternal alleles of this genotype. Allele ID (or
   * index) 0 represents the reference allele. If there are N alternative alleles, then the Alternative Allele ID range
   * is 1 to 0+N. Example, if the a variant has reference allele "A" and the alternative alleles are "[T,GG,ATG]", and
   * the genotype is 2/1, this means the first allele of this genotype has ID 2, which maps to "ATG" and the second
   * allele of this genotype has ID 1, which maps to "GG". TODO: test and verify empty or null alleles
   * 
   * @throws IllegalStateException when an allele from the genotype does not exist in the alternative alleles, or when
   * an allele was not parsed correctly
   * 
   */
  public static List<Integer> convertGenotypeAlleles(final List<Allele> alternativeAlleles, final Genotype genotype) {
    val allelesBuilder = ImmutableList.<Integer> builder();
    for (val allele : genotype.getAlleles()) {
      if (allele.isNonReference()) {
        val indexAltAllele = alternativeAlleles.indexOf(allele);
        val foundIndex = indexAltAllele > -1;
        checkState(foundIndex, "Could not find the allele [%s] in the alternative alleles list [%s] ",
            allele.getBaseString(),
            alternativeAlleles.stream()
                .map(x -> x.getBaseString())
                .collect(Collectors.joining(",")));
        allelesBuilder.add(ALTERNATIVE_ALLELE_INDEX_OFFSET + indexAltAllele);
      } else {
        allelesBuilder.add(DEFAULT_REFERENCE_ALLELE_POSITION);
      }
    }
    val alleles = allelesBuilder.build();
    val hasCorrectNumberOfAlleles = alleles.size() == genotype.getAlleles().size();
    checkState(hasCorrectNumberOfAlleles,
        "There was an error with creating the allele index list. AlternateAlleles: [%s], GenotypesAlleles: [%s]",
        alternativeAlleles.stream().map(x -> x.getBaseString()).collect(Collectors.joining(",")),
        genotype.getAlleles().stream().map(x -> x.getBaseString()).collect(Collectors.joining(",")));
    return alleles;
  }

  // Might want to create a class that decorates EsCallBuilder, where you have EsMuseCallBuilder, EsSangerCallBuilder,
  // etc..
  // and each one has its own implementation of "convertVariantContext". Solves perf problem and avoids giant state
  // machine
  private static List<EsCall> createEsCall(final String callerTypeString,
      final String bioSampleId,
      final VariantContext variantContext) {

    val genotypesContext = variantContext.getGenotypes();
    val commonInfoMap = variantContext.getCommonInfo().getAttributes();
    val altAlleles = variantContext.getAlternateAlleles();

    val callsBuilder = ImmutableList.<EsCall> builder();
    for (val genotype : genotypesContext) {
      val info = genotype.getExtendedAttributes();
      info.putAll(commonInfoMap);
      callsBuilder.add(
          EsCall.builder()
              .variantSetId(callerTypeString)
              .callSetId(bioSampleId)
              .info(info)
              // .sampleName(genotype.getSampleName())
              .genotypeLikelihood(genotype.getLog10PError())
              .isGenotypePhased(genotype.isPhased())
              .nonReferenceAlleles(convertGenotypeAlleles(altAlleles, genotype))
              .build());

    }
    return callsBuilder.build();
  }
}
