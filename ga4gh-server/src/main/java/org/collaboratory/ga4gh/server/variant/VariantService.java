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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsResponse;
import ga4gh.Variants.Call;
import ga4gh.Variants.Variant;
import ga4gh.Variants.Variant.Builder;
import htsjdk.variant.variantcontext.CommonInfo;
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

  private final static int GENOTYPE_NUMBER = 1;
  private final static String PHASESET_VALUE = "true";

  @NonNull
  private final VariantRepository variantRepository;
  @NonNull
  private final HeaderRepository headerRepository;

  @NonNull
  private final CallRepository callRepository;

  private final VCFCodec CODEC = new VCFCodec();
  private final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) {
    val request = SearchVariantsRequest.newBuilder()
        .setEnd(2000000)
        .setStart(0)
        .setPageSize(10)
        .setReferenceName("1")
        .setVariantSetId("consensus")
        .addCallSetIds("SA413898")
        .build();
    val variantRepo = new VariantRepository();
    val headerRepo = new HeaderRepository();
    val callRepo = new CallRepository();

    val variantService = new VariantService(variantRepo, headerRepo, callRepo);
    val searchVariantResponse = variantService.searchVariants(request);
    log.info("SearchVariantResponse: {} ", searchVariantResponse);

  }

  public Variant getVariant(@NonNull GetVariantRequest request) {
    log.info("VariantId to Get: {}", request.getVariantId());
    SearchResponse response = variantRepository.findVariantById(request);
    assert (response.getHits().getTotalHits() == 1);
    return convertToVariant(response.getHits().getAt(0))
        .addAllCalls(
            Streams.stream(
                response.getHits().getAt(0).getInnerHits().get("call"))
                .map(x -> convertToCall(x).build())
                .collect(Collectors.toList()))
        .build();
  }

  public SearchVariantsResponse searchVariants(@NonNull SearchVariantsRequest request) {
    // TODO: This is to explore the request and response fields and is, obviously, not the final implementation
    val nextPageToken = "nextPageToken";

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

  private SearchVariantsResponse buildSearchVariantResponse(@NonNull SearchResponse searchResponse) {
    val responseBuilder = SearchVariantsResponse.newBuilder();

    for (val variantSearchHit : searchResponse.getHits()) {
      val variantBuilder = convertToVariant(variantSearchHit);
      for (val callInnerHit : variantSearchHit.getInnerHits().get("call")) {
        variantBuilder.addCalls(convertToCall(callInnerHit));
      }
      responseBuilder.addVariants(variantBuilder);
    }
    return responseBuilder.build();
  }

  private static Map<String, List<String>> createVariantId2CallSetIdsMap(@NonNull SearchResponse searchResponse) {
    Terms byVariantIdAggTerms = searchResponse.getAggregations().get("by_variant_id");
    val map = new HashMap<String, List<String>>();
    for (Terms.Bucket vb : byVariantIdAggTerms.getBuckets()) {
      val list = new ArrayList<String>();
      Terms byCallsetIdAggTerms = vb.getAggregations().get("by_call_set_id");
      for (Terms.Bucket cb : byCallsetIdAggTerms.getBuckets()) {
        list.add(cb.getKey().toString());
      }
      map.put(vb.getKey().toString(), list);
    }
    return map;
  }

  public static class TypeChecker {

    public static boolean isStringInteger(String input) {
      try {
        Integer.parseInt(input);
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    public static boolean isStringDouble(String input) {
      try {
        Double.parseDouble(input);
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    public static boolean isStringFloat(String input) {
      try {
        Float.parseFloat(input);
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    public static boolean isStringBoolean(String input) {
      try {
        Boolean.parseBoolean(input);
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    public static boolean isObjectBoolean(Object obj) {
      return obj instanceof Boolean;
    }

    public static boolean isObjectInteger(Object obj) {
      return obj instanceof Integer;
    }

    public static boolean isObjectDouble(Object obj) {
      return obj instanceof Double;
    }

    public static boolean isObjectFloat(Object obj) {
      return obj instanceof Float;
    }

    public static boolean isObjectMap(Object obj) {
      return obj instanceof Map<?, ?>;
    }

    public static boolean isObjectCollection(Object obj) {
      return obj instanceof Collection<?>;
    }

  }

  @SuppressWarnings("unchecked")
  private static ListValue createListValueFromObject(Object obj) {
    val listValueBuilder = ListValue.newBuilder();
    if (TypeChecker.isObjectCollection(obj)) {
      for (Object elementObj : (Collection<Object>) obj) {
        listValueBuilder.addValues(Value.newBuilder().setStringValue(elementObj.toString()));
      }
    } else if (TypeChecker.isObjectMap(obj)) { // TODO: still incomplete
      Map<String, Value> map = new HashMap<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
        map.put(entry.getKey().toString(), Value.newBuilder().setStringValue(entry.getValue().toString()).build());
      }
      listValueBuilder.addValues(Value.newBuilder().setStructValue(Struct.newBuilder().putAllFields(map)));
    } else { // Treat everything else as just a string
      listValueBuilder.addValues(Value.newBuilder().setStringValue(obj.toString()).build());
    }
    return listValueBuilder.build();
  }

  private static Map<String, ListValue> createInfo(CommonInfo commonInfo) {
    val map = new HashMap<String, ListValue>();
    for (Map.Entry<String, Object> entry : commonInfo.getAttributes().entrySet()) {
      map.put(entry.getKey(), createListValueFromObject(entry.getValue()));
    }
    return map;
  }

  @SneakyThrows
  private Call.Builder convertToCall(@NonNull SearchHit hit) {
    return Call.newBuilder()
        .setCallSetId(hit.getSource().get("call_set_id").toString())
        .setCallSetName(hit.getSource().get("bio_sample_id").toString()) // TODO: [rtisma] need to add call_set_name to
                                                                         // ES mapping
        .addGenotype(1)
        .addGenotypeLikelihood(0.0)
        .setPhaseset(hit.getSource().get("phaseset").toString());
  }

  // TODO: [rtisma] cleaniup
  // This function is only to be used for searchHits from the SearchVariants service. Its not the same as the GetVariant
  // service
  @SneakyThrows
  private Builder convertToVariant(@NonNull SearchHit hit) {
    JsonNode json = MAPPER.readTree(hit.getSourceAsString());
    // TODO: [rtisma][HACK] - need to find a solution for getting vcfHEader
    val header = new VCFHeader();
    // val response = headerRepository.getHeader(json.get("bio_sample_id").asText());
    // val headerString = response.getSource().get("vcf_header").toString();
    // byte[] data = Base64.getDecoder().decode(headerString);
    // ObjectInputStream ois = new ObjectInputStream(
    // new ByteArrayInputStream(data));
    //
    // val header = (VCFHeader) ois.readObject();
    val codec = new VCFCodec();
    codec.setVCFHeader(header, VCFHeaderVersion.VCF4_1);
    VariantContext vc = codec.decode(json.get("record").asText());

    List<String> alt = vc.getAlternateAlleles().stream()
        .map(al -> al.getBaseString())
        .collect(Collectors.toList());

    Builder builder = Variant.newBuilder()
        .setId(hit.getId())
        .setReferenceName(hit.getSource().get("reference_name").toString())
        .setReferenceBases(vc.getReference().getBaseString())
        .addAllAlternateBases(alt)
        .setStart(vc.getStart())
        .setEnd(vc.getEnd())
        .putAllInfo(createInfo(vc.getCommonInfo()))
        .setCreated(0)
        .setUpdated(0);

    return builder;
  }

}
