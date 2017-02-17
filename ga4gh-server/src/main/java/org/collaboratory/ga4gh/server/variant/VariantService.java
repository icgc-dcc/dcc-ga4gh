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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.collaboratory.ga4gh.core.model.converters.EsCallSetConverter;
import org.collaboratory.ga4gh.core.model.converters.EsCallConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantConverter;
import org.collaboratory.ga4gh.core.model.converters.EsVariantSetConverter;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;

import static org.collaboratory.ga4gh.core.TypeNames.CALL;
import static org.collaboratory.ga4gh.server.util.Protobufs.createInfo;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class VariantService {

  private static final Variant EMPTY_VARIANT = Variant.newBuilder().build();
  private static final VariantSet EMPTY_VARIANT_SET = VariantSet.newBuilder().build();
  private static final CallSet EMPTY_CALL_SET = CallSet.newBuilder().build();
  private static final int FIRST_ELEMENT_POS = 0;
  private final static long DEFAULT_CREATED_VALUE = 0;
  private final static long DEFAULT_UPDATED_VALUE = 0;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @NonNull
  private final VariantRepository variantRepository;

  @NonNull
  private final HeaderRepository headerRepository;

  @NonNull
  private final CallSetRepository callsetRepository;

  @NonNull
  private final VariantSetRepository variantSetRepository;

  @NonNull
  private final EsVariantSetConverter esVariantSetConverter;

  @NonNull
  private final EsCallSetConverter esEsCallSetConverter;

  @NonNull
  private final EsCallConverter esCallConverter;

  @NonNull
  private final EsVariantConverter esVariantConverter;

  /*
   * Variant Processing
   */
  public Variant getVariant(@NonNull GetVariantRequest request) {
    log.info("VariantId to Get: {}", request.getVariantId());
    SearchResponse response = variantRepository.findVariantById(request);
    if (response.getHits().getTotalHits() == 1) {
      val hit = response.getHits().getAt(FIRST_ELEMENT_POS);
      return convertToVariant(hit)
          .addAllCalls(
              stream(
                  hit.getInnerHits().get(CALL))
                  .map(this::convertToCall)
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

  private  Variant.Builder convertToVariant(@NonNull SearchHit hit) {
    val esVariant = esVariantConverter.convertFromSearchHit(hit);

    return Variant.newBuilder()
        .setId(hit.getId())
        .setReferenceName(esVariant.getReferenceName())
        .setReferenceBases(esVariant.getReferenceBases())
        .addAllAlternateBases(esVariant.getAlternativeBases())
        .setStart(esVariant.getStart())
        .setEnd(esVariant.getEnd())
        .setCreated(DEFAULT_CREATED_VALUE)
        .setUpdated(DEFAULT_UPDATED_VALUE);
  }

  private SearchVariantsResponse buildSearchVariantResponse(@NonNull SearchResponse searchResponse) {
    val responseBuilder = SearchVariantsResponse.newBuilder();
    for (val variantSearchHit : searchResponse.getHits()) {
      val variantBuilder = convertToVariant(variantSearchHit);
      for (val callInnerHit : variantSearchHit.getInnerHits().get(CALL)) {
        variantBuilder.addCalls(convertToCall(callInnerHit));
      }
      responseBuilder.addVariants(variantBuilder.build());
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

  private VariantSet convertToVariantSet(final String id, @NonNull Map<String, Object> source) {
    val esVariantSet = esVariantSetConverter.convertFromSource(source);
    return VariantSet.newBuilder()
        .setId(id)
        .setName(esVariantSet.getName())
        .setDatasetId(esVariantSet.getDataSetId())
        .setReferenceSetId(esVariantSet.getReferenceSetId())
        .build();
  }

  private VariantSet convertToVariantSet(@NonNull SearchHit hit) {
    if (hit.hasSource()) {
      return convertToVariantSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_VARIANT_SET;
    }
  }

  private SearchVariantSetsResponse buildSearchVariantSetsResponse(@NonNull SearchResponse response) {
    return SearchVariantSetsResponse.newBuilder()
        .setNextPageToken("N/A")
        .addAllVariantSets(
            Arrays.stream(response.getHits().getHits())
                .map(this::convertToVariantSet)
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

  private CallSet convertToCallSet(final String id, @NonNull Map<String, Object> source) {
    val esCallSet = esEsCallSetConverter.convertFromSource(source);
    return CallSet.newBuilder()
        .setId(id)
        .setName(esCallSet.getName())
        .setBioSampleId(esCallSet.getBioSampleId())
        .addAllVariantSetIds(
            stream( esCallSet.getVariantSetIds())
            .map(Object::toString)
            .collect(toImmutableList()))
        .setCreated(DEFAULT_CREATED_VALUE)
        .setUpdated(DEFAULT_UPDATED_VALUE)
        .build();
  }

  private CallSet convertToCallSet(@NonNull SearchHit hit) {
    if (hit.hasSource()) {
      return convertToCallSet(hit.getId(), hit.getSource());
    } else {
      return EMPTY_CALL_SET;
    }
  }

  private SearchCallSetsResponse buildSearchCallSetsResponse(@NonNull SearchResponse response) {
    return SearchCallSetsResponse.newBuilder()
        .setNextPageToken("N/A")
        .addAllCallSets(
            Arrays.stream(response.getHits().getHits())
                .map(this::convertToCallSet)
                .collect(toImmutableList()))
        .build();
  }

  /*
   * Call Processing
   */
  @SneakyThrows
  private Call convertToCall(@NonNull SearchHit hit) {
    val esCall = esCallConverter.convertFromSearchHit(hit);
    return Call.newBuilder()
        .setCallSetId(Integer.toString(esCall.getCallSetId()))
        .setCallSetName(esCall.getCallSetName())
        .addAllGenotype(esCall.getNonReferenceAlleles())
        .addGenotypeLikelihood(esCall.getGenotypeLikelihood())
        .putAllInfo(createInfo(esCall.getInfo()))
        .setPhaseset(Boolean.toString(esCall.isGenotypePhased()))
        .build();
  }

}
