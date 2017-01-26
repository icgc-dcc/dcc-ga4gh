/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.collaboratory.ga4gh.server.variant;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static org.collaboratory.ga4gh.core.IndexProperties.ALTERNATIVE_BASES;
import static org.collaboratory.ga4gh.core.IndexProperties.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.core.IndexProperties.CALL_SET_ID;
import static org.collaboratory.ga4gh.core.IndexProperties.DATA_SET_ID;
import static org.collaboratory.ga4gh.core.IndexProperties.END;
import static org.collaboratory.ga4gh.core.IndexProperties.GENOTYPE_LIKELIHOOD;
import static org.collaboratory.ga4gh.core.IndexProperties.GENOTYPE_PHASESET;
import static org.collaboratory.ga4gh.core.IndexProperties.INFO;
import static org.collaboratory.ga4gh.core.IndexProperties.NAME;
import static org.collaboratory.ga4gh.core.IndexProperties.NON_REFERENCE_ALLELES;
import static org.collaboratory.ga4gh.core.IndexProperties.REFERENCE_BASES;
import static org.collaboratory.ga4gh.core.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.core.IndexProperties.REFERENCE_SET_ID;
import static org.collaboratory.ga4gh.core.IndexProperties.START;
import static org.collaboratory.ga4gh.core.IndexProperties.VARIANT_SET_IDS;
import static org.collaboratory.ga4gh.server.Factory.newClient;
import static org.collaboratory.ga4gh.server.config.ServerConfig.CALL_TYPE_NAME;
import static org.collaboratory.ga4gh.server.util.Protobufs.createInfo;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsResponse;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsResponse;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsResponse;
import ga4gh.Variants.Call;
import ga4gh.Variants.CallSet;
import ga4gh.Variants.Variant;
import ga4gh.Variants.VariantSet;
import htsjdk.variant.variantcontext.CommonInfo;
import htsjdk.variant.variantcontext.Genotype;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class VariantService {

  private final static long DEFAULT_CALLSET_CREATED_VALUE = 0;
  private final static long DEFAULT_CALLSET_UPDATED_VALUE = 0;
  private final static int DEFAULT_CALL_GENOTYPE_VALUE = 1;
  private final static String DUMMY_PHASESET = "false";
  private final static double DEFAULT_CALL_GENOTYPE_LIKELIHOOD_VALUE = 0.0;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @NonNull
  private final VariantRepository variantRepository;
  @NonNull
  private final HeaderRepository headerRepository;

  @NonNull
  private final CallSetRepository callsetRepository;

  @NonNull
  private final VariantSetRepository variantSetRepository;

  private static final Variant EMPTY_VARIANT = Variant.newBuilder().build();
  private static final VariantSet EMPTY_VARIANT_SET = VariantSet.newBuilder().build();
  private static final CallSet EMPTY_CALL_SET = CallSet.newBuilder().build();
  private static final Call EMPTY_CALL = Call.newBuilder().build();

  public static void main(String[] args) {
    val searchVariantRequest = SearchVariantsRequest.newBuilder()
        .setStart(0)
        .setEnd(10000000)
        .setPageSize(10)
        .setReferenceName("1")
        .setVariantSetId("broad-snowman")
        // .setStart(208764566)
        // .setEnd(208785085)
        // .setPageSize(10)
        // .setReferenceName("1")
        // .setVariantSetId("svcp_1-0-6")
        .build();

    val searchVariantSetRequest = SearchVariantSetsRequest.newBuilder()
        .setPageSize(10)
        .setDatasetId("SSM")
        .build();

    val sampleId = "SA413562";
    val searchCallSetRequest = SearchCallSetsRequest.newBuilder()
        .setVariantSetId("broad-snowman")
        .setBioSampleId(sampleId)
        .setName(sampleId)
        .setPageSize(100)
        .build();

    val getVariantRequest = GetVariantRequest.newBuilder()
        // .setVariantId("27043136_27043136_7_C_T")
        .setVariantId("46421")
        .build();

    val getVariantSetRequest = GetVariantSetRequest.newBuilder()
        .setVariantSetId("1")
        .build();

    val getCallSetRequest = GetCallSetRequest.newBuilder()
        .setCallSetId("1")
        .build();

    val client = newClient();
    val variantRepo = new VariantRepository(client);
    val headerRepo = new HeaderRepository(client);
    val callSetRepo = new CallSetRepository(client);
    val variantSetRepo = new VariantSetRepository(client);

    val variantService = new VariantService(variantRepo, headerRepo, callSetRepo, variantSetRepo);

    val variant = variantService.getVariant(getVariantRequest);
    val searchVariantResponse = variantService.searchVariants(searchVariantRequest);
    val searchVariantSetResponse = variantService.searchVariantSets(searchVariantSetRequest);
    val searchCallSetResponse = variantService.searchCallSets(searchCallSetRequest);
    val callSet = variantService.getCallSet(getCallSetRequest);
    val variantSet = variantService.getVariantSet(getVariantSetRequest);
    log.info("SearchVariantResponse: {} ", searchVariantResponse);
    log.info("SearchVariantSetResponse: {} ", searchVariantSetResponse);
    log.info("SearchCallSetResponse: {} ", searchCallSetResponse);
    log.info("GetVariantSetResponse: {} ", variantSet);
    log.info("GetVariantResponse: {} ", variant);
    log.info("GetCallSetResponse: {} ", callSet);
  }

  public SearchVariantSetsResponse searchVariantSets(@NonNull SearchVariantSetsRequest request) {
    log.info("Getting VariantSetIds for data_set_id: " + request.getDatasetId());
    val response = this.variantSetRepository.findVariantSets(request);
    return buildSearchVariantSetsResponse(response);
  }

  private static SearchVariantSetsResponse buildSearchVariantSetsResponse(@NonNull SearchResponse response) {
    return SearchVariantSetsResponse.newBuilder()
        .setNextPageToken("N/A")
        .addAllVariantSets(
            Arrays.stream(response.getHits().getHits())
                .map(h -> convertToVariantSet(h))
                .collect(toImmutableList()))
        .build();
  }

  private static VariantSet convertToVariantSet(final String id, @NonNull Map<String, Object> source) {
    return VariantSet.newBuilder()
        .setId(id)
        .setName(source.get(NAME).toString())
        .setDatasetId(source.get(DATA_SET_ID).toString())
        .setReferenceSetId(source.get(REFERENCE_SET_ID).toString())
        .build();
  }

  private static CallSet convertToCallSet(final String id, @NonNull Map<String, Object> source) {
    return CallSet.newBuilder()
        .setId(id)
        .setName(source.get(NAME).toString())
        .setBioSampleId(source.get(BIO_SAMPLE_ID).toString())
        // TODO: [rtisma] [BUG] need to properly add variant_set_ids if there is more than one
        .addVariantSetIds(source.get(VARIANT_SET_IDS).toString())
        // .addAlVariantSetIds(
        // Streams.stream(source.).map(vs -> vs.toString())
        // .collect(Collectors.toList()))
        .setCreated(DEFAULT_CALLSET_CREATED_VALUE)
        .setUpdated(DEFAULT_CALLSET_UPDATED_VALUE)
        .build();
  }

  private static VariantSet convertToVariantSet(@NonNull SearchHit hit) {
    if (hit.hasSource()) {
      return convertToVariantSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_VARIANT_SET;
    }
  }

  private static CallSet convertToCallSet(@NonNull SearchHit hit) {
    // TODO: [rtisma] confirm that hasSource means "more than one hit". Point is that it returns an empty response
    if (hit.hasSource()) {
      return convertToCallSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_CALL_SET;
    }
  }

  // TODO: [rtisma] -- should return 204 No content status code if no results. To indicate request was ok, just no
  // results
  public SearchCallSetsResponse searchCallSets(@NonNull SearchCallSetsRequest request) {
    log.info("Getting CallSetIds for variant_set_id: " + request.getVariantSetId());
    val response = callsetRepository.findCallSets(request);
    return buildSearchCallSetsResponse(response);
  }

  private static SearchCallSetsResponse buildSearchCallSetsResponse(@NonNull SearchResponse response) {
    return SearchCallSetsResponse.newBuilder()
        .setNextPageToken("N/A")
        .addAllCallSets(
            Arrays.stream(response.getHits().getHits())
                .map(h -> convertToCallSet(h))
                .collect(toImmutableList()))
        .build();
  }

  public VariantSet getVariantSet(@NonNull GetVariantSetRequest request) {
    log.info("VariantSetId to Get: {}", request.getVariantSetId());
    GetResponse response = variantSetRepository.findVariantSetById(request);
    if (response.isSourceEmpty()) {
      return EMPTY_VARIANT_SET;
    } else {
      return convertToVariantSet(response.getId(), response.getSource());
    }
  }

  public CallSet getCallSet(@NonNull GetCallSetRequest request) {
    log.info("CallSetId to Get: {}", request.getCallSetId());
    GetResponse response = callsetRepository.findCallSetById(request);
    if (response.isSourceEmpty()) {
      return EMPTY_CALL_SET;
    } else {
      return convertToCallSet(response.getId(), response.getSource());
    }
  }

  public Variant getVariant(@NonNull GetVariantRequest request) {
    log.info("VariantId to Get: {}", request.getVariantId());
    SearchResponse response = variantRepository.findVariantById(request);
    if (response.getHits().getTotalHits() == 1) {
      return convertToVariant(response.getHits().getAt(0))
          .addAllCalls(
              Streams.stream(
                  response.getHits().getAt(0).getInnerHits().get(CALL_TYPE_NAME))
                  .map(x -> convertToCall(x).build())
                  .collect(toImmutableList()))
          .build();
    } else {
      return EMPTY_VARIANT;
    }
  }

  public SearchVariantsResponse searchVariants(@NonNull SearchVariantsRequest request) {
    // TODO: This is to explore the request and response fields and is, obviously, not the final implementation

    log.info("pageToken: {}", request.getPageToken());
    log.info("pageSize: {}", request.getPageSize());
    log.info("referenceName: {}", request.getReferenceName());
    log.info("variantSetId: {}", request.getVariantSetId());
    log.info("callSetIdsList: {}", request.getCallSetIdsList());
    log.info("start: {}", request.getStart());
    log.info("end: {}", request.getEnd());

    val response = variantRepository.findVariants(request);
    return buildSearchVariantResponse(response);
  }

  private static SearchVariantsResponse buildSearchVariantResponse(@NonNull SearchResponse searchResponse) {
    val responseBuilder = SearchVariantsResponse.newBuilder();
    for (val variantSearchHit : searchResponse.getHits()) {
      val variantBuilder = convertToVariant(variantSearchHit);
      for (val callInnerHit : variantSearchHit.getInnerHits().get(CALL_TYPE_NAME)) {
        variantBuilder.addCalls(convertToCall(callInnerHit));
      }
      responseBuilder.addVariants(variantBuilder);
    }
    return responseBuilder.build();
  }

  private static List<Integer> convertNonRefAlleles(@NonNull final Genotype genotype) {
    // TODO: [rtisma] -- verify this logic is correct. Allele has some other states that might need to be considered
    val allelesBuilder = ImmutableList.<Integer> builder();
    for (int i = 0; i < genotype.getAlleles().size(); i++) {
      val allele = genotype.getAllele(i);
      if (allele.isNonReference()) {
        allelesBuilder.add(i);
      }
    }
    return allelesBuilder.build();
  }

  private static void mergeAttributes(@NonNull final CommonInfo info, Map<String, ?> attributes) {
    info.putAttributes(attributes);
  }

  @SneakyThrows
  private static Call.Builder convertToCall(@NonNull SearchHit hit) {
    val source = hit.getSource();
    val callSetId = source.get(CALL_SET_ID).toString();
    val callSetName = source.get(CALL_SET_ID).toString();

    // TODO: [BUG] this isnt working properly. Just taking last element
    val nonRefAlleles = getHitArray(hit, NON_REFERENCE_ALLELES).stream()
        .map(o -> parseInt(o.toString()))
        .collect(toImmutableList());
    val genotypeLikelihood = parseDouble(source.get(GENOTYPE_LIKELIHOOD).toString());
    val genotypePhaseset = source.get(GENOTYPE_PHASESET).toString();

    // Working properly
    val callInfo = getHitMap(hit, INFO);

    // TODO: [rtisma] need to extract data from genotype and put into call
    return Call.newBuilder()
        .setCallSetId(callSetId)
        .setCallSetName(callSetName)
        .addAllGenotype(nonRefAlleles)
        .addGenotypeLikelihood(genotypeLikelihood)
        .putAllInfo(createInfo(callInfo))
        .setPhaseset(genotypePhaseset);
  }

  private static String getHitAsString(final SearchHit hit, String attr) {
    return hit.getSource().get(attr).toString();
  }

  private static Long getHitAsLong(final SearchHit hit, String attr) {
    return Long.parseLong(hit.getSource().get(attr).toString());
  }

  // TODO: [rtisma] verify this is the correct way to extract an array result
  @SuppressWarnings("unchecked")
  private static List<Object> getHitArray(final SearchHit hit, String field) {
    return (List<Object>) hit.getSource().get(field);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> getHitMap(final SearchHit hit, String field) {
    return (Map<String, Object>) hit.getSource().get(field);
  }

  @SneakyThrows
  private static Variant.Builder convertToVariant(@NonNull SearchHit hit) {
    List<String> alternateBases = getHitArray(hit, ALTERNATIVE_BASES).stream()
        .map(o -> o.toString())
        .collect(toImmutableList());

    return Variant.newBuilder()
        .setId(hit.getId())
        .setReferenceName(getHitAsString(hit, REFERENCE_NAME))
        .setReferenceBases(getHitAsString(hit, REFERENCE_BASES))
        .addAllAlternateBases(alternateBases)
        .setStart(getHitAsLong(hit, START))
        .setEnd(getHitAsLong(hit, END))
        .setCreated(0)
        .setUpdated(0);
  }
}
