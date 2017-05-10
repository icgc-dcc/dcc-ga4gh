package org.icgc.dcc.ga4gh.loader;

import ga4gh.VariantServiceOuterClass.GetCallSetRequest;
import ga4gh.VariantServiceOuterClass.GetVariantRequest;
import ga4gh.VariantServiceOuterClass.GetVariantSetRequest;
import ga4gh.VariantServiceOuterClass.SearchCallSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantSetsRequest;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson2;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.server.config.ServerConfig;
import org.icgc.dcc.ga4gh.server.variant.CallSetRepository;
import org.icgc.dcc.ga4gh.server.variant.HeaderRepository;
import org.icgc.dcc.ga4gh.server.variant.VariantRepository;
import org.icgc.dcc.ga4gh.server.variant.VariantService;
import org.icgc.dcc.ga4gh.server.variant.VariantSetRepository;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.icgc.dcc.ga4gh.server.Factory.newClient;

/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
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

@Slf4j
@Ignore
@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class VariantServiceTest {

  private static VariantService variantService;
  private static Client client;

  @BeforeClass
  public static void init() {
    log.info("Config: \n{}", ServerConfig.toConfigString());
    try {
      client = newClient();
      val variantRepo = new VariantRepository(client);
      val headerRepo = new HeaderRepository(client);
      val callSetRepo = new CallSetRepository(client);
      val variantSetRepo = new VariantSetRepository(client);
      val esVariantConverter = new EsVariantConverterJson();
      val esVariantSetConverter = new EsVariantSetConverterJson();
      val esCallSetConverter = new EsCallSetConverterJson();
      val esCallConverter = new EsCallConverterJson();
      val esVariantCallPairConverter = new EsVariantCallPairConverterJson2(esVariantConverter
      , esCallConverter, esVariantConverter, esCallConverter);

      variantService =
          new VariantService(variantRepo, headerRepo, callSetRepo, variantSetRepo, esVariantSetConverter, esCallSetConverter, esVariantCallPairConverter);
    } catch (Exception e) {
      log.error("Message[{}] : {}\nStackTrace: {}", e.getClass().getName(), e.getMessage(), e);
    }
  }

  @AfterClass
  public static void shutdown() {
    client.close();
  }


  public static void main(String[] args) {
    val esVariantConverter = new EsVariantConverterJson();
    val esVariantSetConverter = new EsVariantSetConverterJson();
    val esCallSetConverter = new EsCallSetConverterJson();
    val esCallConverter = new EsCallConverterJson();
    val esVariantCallPairConverter = new EsVariantCallPairConverterJson2(esVariantConverter,esCallConverter,esVariantConverter, esCallConverter);
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

    val variantService = new VariantService(variantRepo, headerRepo, callSetRepo, variantSetRepo, esVariantSetConverter, esCallSetConverter, esVariantCallPairConverter);

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

  @Test
  public void testGetVariant() {
    val getVariantRequest = GetVariantRequest.newBuilder()
        // .setVariantId("27043136_27043136_7_C_T")
        .setVariantId("7933202_7933203_1_TA_T")
        .build();
    val variant = variantService.getVariant(getVariantRequest);
    log.info("GetVariantResponse: {} ", variant);
  }

  @Test
  public void testSearchVariants(){
    val searchVariantRequest = SearchVariantsRequest.newBuilder()
        .setVariantSetId("3")
        .addCallSetIds("1734")
        .setStart(0)
        .setEnd(1000000)
        .setPageSize(3)
        .setReferenceName("1")
        .build();

    val searchVariantsResponse = variantService.searchVariants(searchVariantRequest);
    int count = 0;
    for (val variant : searchVariantsResponse.getVariantsList()){
      log.info("SearchVariantResponse [{}]: {} ", ++count, variant);
    }
  }

  @Test
  public void testSearchVariantSets() {
    val searchVariantSetsRequest = SearchVariantSetsRequest.newBuilder()
        .setDatasetId("SSM")
        .setPageSize(100)
        .build();

    val searchVariantSetsResponse = variantService.searchVariantSets(searchVariantSetsRequest);
    int count = 0;
    for (val variantSet : searchVariantSetsResponse.getVariantSetsList()) {
      log.info("SearchVariantSetsResponse [{}]: {} ", ++count, variantSet);
    }
    log.info("TotalCount: {}", searchVariantSetsResponse.getVariantSetsCount());
  }

  @Test
  public void testGetVariantSet() {
    val getVariantSetRequest = GetVariantSetRequest.newBuilder()
        .setVariantSetId("3")
        .build();

    val variantSet = variantService.getVariantSet(getVariantSetRequest);
    log.info("VariantSet: {} ", variantSet);
  }

  @Test
  public void testGetCallSet() {
    val getCallSetRequest = GetCallSetRequest.newBuilder()
        .setCallSetId("3")
        .build();

    val callSet = variantService.getCallSet(getCallSetRequest);
    log.info("CallSet: {} ", callSet);
  }

  @Test
  public void testSearchCallSets() {
    val searchCallSetsRequest = SearchCallSetsRequest.newBuilder()
        .setName("SA528997")
        .setBioSampleId("SA528997")
        .setVariantSetId("5")
        .setPageSize(100)
        .build();

    val searchCallSetsResponse = variantService.searchCallSets(searchCallSetsRequest);
    int count = 0;
    for (val callSet : searchCallSetsResponse.getCallSetsList()) {
      log.info("SearchCallSetsResponse [{}]: {} ", ++count, callSet);
    }
    log.info("TotalCount: {}", searchCallSetsResponse.getCallSetsCount());
  }
}
