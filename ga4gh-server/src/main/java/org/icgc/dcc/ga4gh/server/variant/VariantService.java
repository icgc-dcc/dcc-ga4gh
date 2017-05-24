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
package org.icgc.dcc.ga4gh.server.variant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.UnmodifiableLazyStringList;
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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.es.EsConsensusCall;
import org.icgc.dcc.ga4gh.server.util.Protobufs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.ga4gh.common.PropertyNames.VARIANT_SET_IDS;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class VariantService {

  private static final Set<String> EMPTY_STRING_SET = newHashSet();
  private static final Variant EMPTY_VARIANT = Variant.newBuilder().build();
  private static final VariantSet EMPTY_VARIANT_SET = VariantSet.newBuilder().build();
  private static final CallSet EMPTY_CALL_SET = CallSet.newBuilder().build();
  private static final int FIRST_ELEMENT_POS = 0;
  private final static long DEFAULT_CREATED_VALUE = 0;
  private final static long DEFAULT_UPDATED_VALUE = 0;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<Integer> DEFAULT_CONSENSUS_NON_REF_ALLELES = newArrayList(-1);
  private static final double DEFAULT_CONSENSUS_GENOTYPE_LIKELIHOOD = -1.0;
  private static final boolean DEFAULT_CONSENSUS_GENOTYPE_PHASED = false;


  @NonNull
  private final VariantRepository variantRepository;

  @NonNull
  private final HeaderRepository headerRepository;

  @NonNull
  private final CallSetRepository callsetRepository;

  @NonNull
  private final VariantSetRepository variantSetRepository;

  @NonNull
  private final EsVariantSetConverterJson esVariantSetConverter;

  @NonNull
  private final EsCallSetConverterJson esEsCallSetConverter;

  @NonNull
  private final EsVariantCallPairConverterJson esVariantCallPairConverter;

  /*
   * Variant Processing
   */
  public Variant getVariant(@NonNull GetVariantRequest request) {
    log.info("VariantId to Get: {}", request.getVariantId());
    GetResponse response = variantRepository.findVariantById(request);
    if (response.isSourceEmpty()) {
      return EMPTY_VARIANT;
    } else {
      return convertToVariant(response.getId(), response.getSource());
    }
  }

  public SearchVariantsResponse searchVariants(@NonNull SearchVariantsRequest request) {
    // TODO: This is to explore the request and response fields and is, obviously, not the final implementation

    // log.info("pageToken: {}", request.getPageToken());
    // log.info("pageSize: {}", request.getPageSize());
    // log.info("referenceName: {}", request.getReferenceName());
    // log.info("variantSetId: {}", request.getVariantSetId());
    // log.info("callSetIdsList: {}", request.getCallSetIdsList());
    // log.info("start: {}", request.getStart());
    // log.info("end: {}", request.getEnd());

    val response = variantRepository.findVariants(request);
    if (request.getCallSetIdsCount() > 0){
      val callsetIds = newHashSet(((UnmodifiableLazyStringList) request.getCallSetIdsList()).getUnmodifiableView());
      return buildSearchVariantResponse(response, callsetIds);
    } else {
      return buildSearchVariantResponse(response, EMPTY_STRING_SET);
    }
  }


  private Variant convertToVariant(final String id, @NonNull Map<String, Object> source) {
    return convertToVariant(id, source, EMPTY_STRING_SET);
  }

  private Variant convertToVariant(final String id, @NonNull Map<String, Object> source, Set<String> allowedCallSetIds) {
    val esVariantCallPair = esVariantCallPairConverter.convertFromSource(source, allowedCallSetIds);
    val esVariant = esVariantCallPair.getVariant();

    val variantBuilder = Variant.newBuilder()
        .setId(id)
        .setReferenceName(esVariant.getReferenceName())
        .setReferenceBases(esVariant.getReferenceBases())
        .addAllAlternateBases(esVariant.getAlternativeBases())
        .setStart(esVariant.getStart())
        .setEnd(esVariant.getEnd())
        .setCreated(DEFAULT_CREATED_VALUE)
        .setUpdated(DEFAULT_UPDATED_VALUE);


    esVariantCallPair.getCalls()
        .stream()
        .map(this::convertToCall)
        .forEach(variantBuilder::addCalls);
    return variantBuilder.build();
  }

  private Variant convertToVariant(@NonNull SearchHit hit, Set<String> allowedCallSetIds) {
    if (hit.hasSource()) {
      return convertToVariant(hit.getId(), hit.getSource(), allowedCallSetIds);
    } else {
      return EMPTY_VARIANT;
    }
  }

  private Variant convertToVariant(@NonNull SearchHit hit) {
    return convertToVariant(hit, EMPTY_STRING_SET);
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

  private SearchVariantsResponse buildSearchVariantResponse(@NonNull SearchResponse searchResponse, Set<String> allowedCallSetIds) {
//    val callsetIds = searchResponse.
    return SearchVariantsResponse.newBuilder()
        .setNextPageToken("N/A")
        .addAllVariants(
            stream(searchResponse.getHits())
            .map(x -> convertToVariant(x, allowedCallSetIds))
            .collect(toImmutableList())
        )
        .build();
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
  private Call convertToCall(@NonNull EsConsensusCall esBasicCall) {
    val variantSetIds = esBasicCall.getVariantSetIds();
    val info = esBasicCall.getInfo();
    info.put(VARIANT_SET_IDS, variantSetIds);
    return Call.newBuilder()
        .setCallSetId(Integer.toString(esBasicCall.getCallSetId()))
        .setCallSetName(esBasicCall.getCallSetName())
        .addAllGenotype(DEFAULT_CONSENSUS_NON_REF_ALLELES)
        .addGenotypeLikelihood(DEFAULT_CONSENSUS_GENOTYPE_LIKELIHOOD)
        .putAllInfo(Protobufs.createInfo(info))
        .setPhaseset(Boolean.toString(DEFAULT_CONSENSUS_GENOTYPE_PHASED))
        .build();
  }

}
