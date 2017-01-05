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

import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.BIO_SAMPLE_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.BY_DATA_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.CALL_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.DATA_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.RECORD;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_NAME;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.REFERENCE_SET_ID;
import static org.collaboratory.ga4gh.resources.mappings.IndexProperties.VARIANT_SET_IDS;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ga4gh.Metadata.Dataset;
import ga4gh.MetadataServiceOuterClass.SearchDatasetsRequest;
import ga4gh.MetadataServiceOuterClass.SearchDatasetsResponse;
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
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
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

  public static void main(String[] args) {
    val searchVariantRequest = SearchVariantsRequest.newBuilder()
        .setEnd(50000000)
        .setStart(0)
        .setPageSize(10)
        .setReferenceName("7")
        .setVariantSetId("consensus")
        .addCallSetIds("SA557454")
        .build();

    val searchVariantSetRequest = SearchVariantSetsRequest.newBuilder()
        .setDatasetId("SSM")
        .build();

    val searchCallSetRequest = SearchCallSetsRequest.newBuilder()
        .setVariantSetId("consensus")
        .setBioSampleId("SA557454")
        .setName("SA557454")
        .setPageSize(100)
        .build();

    val getVariantRequest = GetVariantRequest.newBuilder()
        .setVariantId("27043136_27043136_7_C_T")
        .build();

    val getVariantSetRequest = GetVariantSetRequest.newBuilder()
        .setVariantSetId("consensus")
        .build();

    val getCallSetRequest = GetCallSetRequest.newBuilder()
        .setCallSetId("SA557454")
        .build();

    val client = newClient();
    val variantRepo = new VariantRepository(client);
    val headerRepo = new HeaderRepository(client);
    val callSetRepo = new CallSetRepository(client);
    val variantSetRepo = new VariantSetRepository(client);

    val variantService = new VariantService(variantRepo, headerRepo, callSetRepo, variantSetRepo);
    val searchVariantResponse = variantService.searchVariants(searchVariantRequest);
    val searchVariantSetResponse = variantService.searchVariantSets(searchVariantSetRequest);
    val searchCallSetResponse = variantService.searchCallSets(searchCallSetRequest);
    val callSet = variantService.getCallSet(getCallSetRequest);
    val variantSet = variantService.getVariantSet(getVariantSetRequest);
    val variant = variantService.getVariant(getVariantRequest);
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
                .map(h -> convertToVariantSet(h).build())
                .collect(toImmutableList()))
        .build();
  }

  private static VariantSet.Builder convertToVariantSet(final String id, @NonNull Map<String, Object> source) {
    return VariantSet.newBuilder()
        .setId(id)
        .setName(source.get(NAME).toString())
        .setDatasetId(source.get(DATA_SET_ID).toString())
        .setReferenceSetId(source.get(REFERENCE_SET_ID).toString());
  }

  private static CallSet.Builder convertToCallSet(final String id, @NonNull Map<String, Object> source) {
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
        .setUpdated(DEFAULT_CALLSET_UPDATED_VALUE);
  }

  private static VariantSet.Builder convertToVariantSet(@NonNull SearchHit hit) {
    return convertToVariantSet(hit.getId(), hit.getSource());
  }

  private static CallSet.Builder convertToCallSet(@NonNull SearchHit hit) {
    return convertToCallSet(hit.getId(), hit.getSource());
  }

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
                .map(h -> convertToCallSet(h).build())
                .collect(toImmutableList()))
        .build();
  }

  public VariantSet getVariantSet(@NonNull GetVariantSetRequest request) {
    log.info("VariantSetId to Get: {}", request.getVariantSetId());
    GetResponse response = variantSetRepository.findVariantSetById(request);
    return convertToVariantSet(response.getId(), response.getSource()).build();
  }

  public CallSet getCallSet(@NonNull GetCallSetRequest request) {
    log.info("CallSetId to Get: {}", request.getCallSetId());
    GetResponse response = callsetRepository.findCallSetById(request);
    return convertToCallSet(response.getId(), response.getSource()).build();
  }

  public Variant getVariant(@NonNull GetVariantRequest request) {
    log.info("VariantId to Get: {}", request.getVariantId());
    SearchResponse response = variantRepository.findVariantById(request);
    assert (response.getHits().getTotalHits() == 1);
    return convertToVariant(response.getHits().getAt(0))
        .addAllCalls(
            Streams.stream(
                response.getHits().getAt(0).getInnerHits().get(CALL_TYPE_NAME))
                .map(x -> convertToCall(x).build())
                .collect(toImmutableList()))
        .build();
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

  public SearchDatasetsResponse searchDatasets(@NonNull SearchDatasetsRequest request) {
    val response = variantSetRepository.searchAllDataSets(request);
    return buildSearchDatasetsResponse(response);
  }

  private static SearchDatasetsResponse buildSearchDatasetsResponse(@NonNull SearchResponse searchResponse) {
    Terms datasets = searchResponse.getAggregations().get(BY_DATA_SET_ID);
    return SearchDatasetsResponse.newBuilder()
        .addAllDatasets(datasets.getBuckets().stream()
            .map(b -> Dataset.newBuilder()
                .setId(b.getKey().toString())
                .setName(b.getKey().toString())
                .build())
            .collect(toImmutableList()))
        .build();
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

  @SneakyThrows
  private static Call.Builder convertToCall(@NonNull SearchHit hit) {
    return Call.newBuilder()
        .setCallSetId(hit.getSource().get(CALL_SET_ID).toString())
        .setCallSetName(hit.getSource().get(BIO_SAMPLE_ID).toString()) // TODO: [rtisma] need to add call_set_name to
                                                                       // ES mapping
        .addGenotype(DEFAULT_CALL_GENOTYPE_VALUE)
        .addGenotypeLikelihood(DEFAULT_CALL_GENOTYPE_LIKELIHOOD_VALUE)
        .putAllInfo(values)
        .setPhaseset(DUMMY_PHASESET);
  }

  // TODO: [rtisma] cleaniup
  // This function is only to be used for searchHits from the SearchVariants service. Its not the same as the GetVariant
  // service
  @SneakyThrows
  private static Variant.Builder convertToVariant(@NonNull SearchHit hit) {
    JsonNode json = MAPPER.readTree(hit.getSourceAsString());
    // TODO: [rtisma][HACK] - need to find a solution for getting vcfHEader
    val dummyHeader = new VCFHeader();
    val codec = new VCFCodec();
    codec.setVCFHeader(dummyHeader, VCFHeaderVersion.VCF4_1);
    VariantContext vc = codec.decode(json.get(RECORD).asText());

    List<String> alt = vc.getAlternateAlleles().stream()
        .map(al -> al.getBaseString())
        .collect(toImmutableList());

    return Variant.newBuilder()
        .setId(hit.getId())
        .setReferenceName(hit.getSource().get(REFERENCE_NAME).toString())
        .setReferenceBases(vc.getReference().getBaseString())
        .addAllAlternateBases(alt)
        .setStart(vc.getStart())
        .setEnd(vc.getEnd())
        .putAllInfo(createInfo(vc.getCommonInfo()))
        .setCreated(0)
        .setUpdated(0);
  }

}
