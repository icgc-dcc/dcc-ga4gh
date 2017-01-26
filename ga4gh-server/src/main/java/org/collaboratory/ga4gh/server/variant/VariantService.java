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
import static org.collaboratory.ga4gh.server.config.ServerConfig.CALL_TYPE_NAME;
import static org.collaboratory.ga4gh.server.util.Protobufs.createInfo;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToDouble;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToIntegerList;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToLong;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToObjectMap;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToString;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertHitToStringList;
import static org.collaboratory.ga4gh.server.util.SearchHitConverters.convertSourceToString;
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

  /*
   * Variant Processing
   */
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

  private static Variant.Builder convertToVariant(@NonNull SearchHit hit) {
    List<String> alternateBases = convertHitToStringList(hit, ALTERNATIVE_BASES);

    return Variant.newBuilder()
        .setId(hit.getId())
        .setReferenceName(convertHitToString(hit, REFERENCE_NAME))
        .setReferenceBases(convertHitToString(hit, REFERENCE_BASES))
        .addAllAlternateBases(alternateBases)
        .setStart(convertHitToLong(hit, START))
        .setEnd(convertHitToLong(hit, END))
        .setCreated(0)
        .setUpdated(0);
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

  /*
   * VariantSet Processing
   */
  public VariantSet getVariantSet(@NonNull GetVariantSetRequest request) {
    log.info("VariantSetId to Get: {}", request.getVariantSetId());
    GetResponse response = variantSetRepository.findVariantSetById(request);
    if (response.isSourceEmpty()) {
      return EMPTY_VARIANT_SET;
    } else {
      return convertToVariantSet(response.getId(), response.getSource());
    }
  }

  public SearchVariantSetsResponse searchVariantSets(@NonNull SearchVariantSetsRequest request) {
    log.info("Getting VariantSetIds for data_set_id: " + request.getDatasetId());
    val response = this.variantSetRepository.findVariantSets(request);
    return buildSearchVariantSetsResponse(response);
  }

  private static VariantSet convertToVariantSet(final String id, @NonNull Map<String, Object> source) {
    return VariantSet.newBuilder()
        .setId(id)
        .setName(convertSourceToString(source, NAME))
        .setDatasetId(convertSourceToString(source, DATA_SET_ID))
        .setReferenceSetId(convertSourceToString(source, REFERENCE_SET_ID))
        .build();
  }

  private static VariantSet convertToVariantSet(@NonNull SearchHit hit) {
    if (hit.hasSource()) {
      return convertToVariantSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_VARIANT_SET;
    }
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

  /*
   * CallSet Processing
   */
  // TODO: [rtisma] -- should return 204 No content status code if no results. To indicate request was ok, just no
  // results
  public CallSet getCallSet(@NonNull GetCallSetRequest request) {
    log.info("CallSetId to Get: {}", request.getCallSetId());
    GetResponse response = callsetRepository.findCallSetById(request);
    if (response.isSourceEmpty()) {
      return EMPTY_CALL_SET;
    } else {
      return convertToCallSet(response.getId(), response.getSource());
    }
  }

  public SearchCallSetsResponse searchCallSets(@NonNull SearchCallSetsRequest request) {
    log.info("Getting CallSetIds for variant_set_id: " + request.getVariantSetId());
    val response = callsetRepository.findCallSets(request);
    return buildSearchCallSetsResponse(response);
  }

  private static CallSet convertToCallSet(final String id, @NonNull Map<String, Object> source) {
    return CallSet.newBuilder()
        .setId(id)
        .setName(convertSourceToString(source, NAME))
        .setBioSampleId(convertSourceToString(source, BIO_SAMPLE_ID))
        // TODO: [rtisma] [BUG] need to properly add variant_set_ids if there is more than one
        .addVariantSetIds(convertSourceToString(source, VARIANT_SET_IDS))
        // .addAlVariantSetIds(
        // Streams.stream(source.).map(vs -> vs.toString())
        // .collect(Collectors.toList()))
        .setCreated(DEFAULT_CALLSET_CREATED_VALUE)
        .setUpdated(DEFAULT_CALLSET_UPDATED_VALUE)
        .build();
  }

  private static CallSet convertToCallSet(@NonNull SearchHit hit) {
    // TODO: [rtisma] confirm that hasSource means "more than one hit". Point is that it returns an empty response
    if (hit.hasSource()) {
      return convertToCallSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_CALL_SET;
    }
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

  /*
   * Call Processing
   */
  @SneakyThrows
  private static Call.Builder convertToCall(@NonNull SearchHit hit) {
    val callSetId = convertHitToString(hit, CALL_SET_ID);
    val callSetName = convertHitToString(hit, CALL_SET_ID);

    // TODO: [BUG] this isnt working properly. Just taking last element
    val nonRefAlleles = convertHitToIntegerList(hit, NON_REFERENCE_ALLELES);
    val genotypeLikelihood = convertHitToDouble(hit, GENOTYPE_LIKELIHOOD);
    val genotypePhaseset = convertHitToString(hit, GENOTYPE_PHASESET);

    // Working properly
    val callInfo = convertHitToObjectMap(hit, INFO);

    return Call.newBuilder()
        .setCallSetId(callSetId)
        .setCallSetName(callSetName)
        .addAllGenotype(nonRefAlleles)
        .addGenotypeLikelihood(genotypeLikelihood)
        .putAllInfo(createInfo(callInfo))
        .setPhaseset(genotypePhaseset);
  }

}
