package org.icgc.dcc.ga4gh.loader.vcf;

import com.fasterxml.jackson.databind.node.ObjectNode;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsVariantCallPair;
import org.icgc.dcc.ga4gh.loader.model.metadata.FileMetaData;
import org.icgc.dcc.ga4gh.loader.utils.CounterMonitor;
import org.icgc.dcc.ga4gh.loader.utils.cache.id.IdCache;
import org.icgc.dcc.ga4gh.loader.vcf.callprocessors.CallProcessor;
import org.icgc.dcc.ga4gh.loader.vcf.enums.MutationTypes;
import org.icgc.dcc.ga4gh.loader.vcf.enums.SubMutationTypes;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.ga4gh.common.PropertyNames.BIO_SAMPLE_ID;
import static org.icgc.dcc.ga4gh.common.MiscNames.DONOR_ID;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_ID;
import static org.icgc.dcc.ga4gh.common.MiscNames.VCF_HEADER;
import static org.icgc.dcc.ga4gh.loader.utils.CounterMonitor.newMonitor;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@Slf4j
public class VCF implements Closeable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;

  /*
   * Dependancies
   */
  @Getter
  private final VCFFileReader vcf;

  @Getter
  private final FileMetaData fileMetaData;

  @Getter
  private final CallProcessor callProcessor;

  private final EsVariantConverterJson variantConverter;

  /*
   * Cached for speed
   */
  private final String variantSetName;
  private final String callSetName;
  private final String mutationTypeString;
  private final String mutationSubTypeString;
  private final boolean isMutationTypesCorrect;
  private final int variantSetId;
  private final int callSetId;

  /*
   * State
   */
  private final CounterMonitor variantCallPairMonitor = newMonitor("VariantCallPairParsing", 250000);
  private final VCFEncoder encoder;

  // TODO: eliminate IdCache dependency and just inject variantSetId and callSetId directly.
  // Create another class to do the extraction of callsetId and variantSetId and which constructs VCF.
  // Currently really hard to test ( or atleast there is alot of mocking that has to be done)
  public VCF(@NonNull final File file,
      @NonNull final FileMetaData fileMetaData,
      @NonNull final IdCache<String, Integer> variantSetIdCache,
      @NonNull final IdCache<String, Integer> callSetIdCache,
      @NonNull final CallProcessor callProcessor,
      @NonNull final EsVariantConverterJson variantConverter) {
    this.vcf = new VCFFileReader(file, REQUIRE_INDEX_CFG);
    this.variantConverter = variantConverter;
    this.fileMetaData = fileMetaData;
    this.callProcessor = callProcessor;
    this.encoder = new VCFEncoder(vcf.getFileHeader(),

        ALLOW_MISSING_FIELDS_IN_HEADER_CFG,

        OUTPUT_TRAILING_FORMAT_FIELDS_CFG);

    val parser = fileMetaData.getVcfFilenameParser();

    this.variantSetName = parser.getCallerId();
    this.callSetName = fileMetaData.getSampleId();

    this.mutationTypeString = parser.getMutationType();
    this.mutationSubTypeString = parser.getSubMutationType();
    this.isMutationTypesCorrect = isMutationTypesCorrect(mutationTypeString, mutationSubTypeString);

//    checkState(isMutationTypesCorrect,
//        "Error: the mutationType(%s) must be of type [%s], and the subMutationType(%s) can be eitheror of [%s ,%s]",
//        mutationTypeString,
//        MutationTypes.somatic,
//        mutationSubTypeString,
//        SubMutationTypes.indel,
//        SubMutationTypes.snv_mnv);

    val variantSetNameExistsInCache = variantSetIdCache.contains(variantSetName);
    val callSetNameExistsInCache = callSetIdCache.contains(callSetName);
    checkState(variantSetNameExistsInCache, "VariantSetName [{}] does not exist in the variantSetIdCache",
        variantSetName);
    checkState(callSetNameExistsInCache, "CallSetName [{}] does not exist in the callSetIdCache",
        callSetName);

    this.variantSetId = variantSetIdCache.getId(variantSetName);
    this.callSetId = callSetIdCache.getId(callSetName);

  }

  public Stream<EsVariantCallPair> readVariantAndCalls() {
    return stream(vcf.iterator())
        .map(this::convertVariantCallPair);
  }

  public VCFHeader getHeader() {
    return vcf.getFileHeader();
  }

  private static boolean isMutationTypesCorrect(String mutationTypeString, String mutationSubTypeString) {
    return MutationTypes.somatic.equals(mutationTypeString) && (SubMutationTypes.indel.equals(mutationSubTypeString)
        || SubMutationTypes.snv_mnv.equals(mutationSubTypeString));
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
        .with(BIO_SAMPLE_ID, callSetId)
        .with(VARIANT_SET_ID, variantSetId)
        .end();
  }

  private EsVariantCallPair convertVariantCallPair(final VariantContext record) {
    variantCallPairMonitor.start();
    val variant = variantConverter.convertFromVariantContext(record);
    val calls = callProcessor.createEsCallList(variantSetId, callSetId, callSetName, record);
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

}
