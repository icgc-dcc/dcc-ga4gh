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

package org.icgc.dcc.ga4gh.server.performance;

import com.google.common.base.Stopwatch;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import ga4gh.VariantServiceOuterClass;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsConsensusCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.server.config.ServerConfig;
import org.icgc.dcc.ga4gh.server.performance.random.SVRRandomGenerator;
import org.icgc.dcc.ga4gh.server.variant.CallSetRepository;
import org.icgc.dcc.ga4gh.server.variant.HeaderRepository;
import org.icgc.dcc.ga4gh.server.variant.VariantRepository;
import org.icgc.dcc.ga4gh.server.variant.VariantService;
import org.icgc.dcc.ga4gh.server.variant.VariantSetRepository;

import java.io.FileWriter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.common.core.util.Joiners.SEMICOLON;
import static org.icgc.dcc.ga4gh.server.Factory.newClient;
import static org.icgc.dcc.ga4gh.server.config.ServerConfig.INDEX_NAME;
import static org.icgc.dcc.ga4gh.server.performance.Performance.MinMax.createMinMax;
import static org.icgc.dcc.ga4gh.server.performance.random.SVRRandomGenerator.createSVRRandomGenerator;
import static org.icgc.dcc.ga4gh.server.performance.random.UniConstrainedRandomIntegerGenerator.createUniConstrainedRandomIntegerGenerator;
import static org.icgc.dcc.ga4gh.server.performance.random.UniConstrainedRandomStringGenerator.createUniConstrainedRandomStringGenerator;

@Builder
@RequiredArgsConstructor
@Slf4j
public class Performance implements Runnable {
  private final static char CSV_DELIMITER = ',';

  /*
  public static List<EsResult> createEsResultList(Client client){
    val query = QueryBuilders.matchAllQuery();
    val aggs = AggregationBuilders.terms("byReferenceName").field("reference_name")
        .subAggregation(AggregationBuilders.max("maxEnd").field("end"))
        .subAggregation(AggregationBuilders.min("minStart").field("start"));
    val aggregations = client.prepareSearch(INDEX_NAME).setQuery(query).addAggregation(aggs).execute().actionGet().getAggregations();
    val terms = (Terms)aggregations.get("byReferenceName");

    val map = ImmutableMap.<String, SVRSweeper>builder();
    for (Terms.Bucket bucket : terms.getBuckets()){
      val referenceName = bucket.getKeyAsString();
      val minStartTerm = (Min)bucket.getAggregations().get("minStart");
      val minStart = (int)minStartTerm.getValue();

      val maxEndTerm = (Max)bucket.getAggregations().get("maxEnd");
      val maxEnd = (int)maxEndTerm.getValue();
      val it = createSVRSweeper(referenceName,minStart,maxEnd,variantLength);
      map.put(referenceName, it);
    }
  }
  */

  @Value
  public static class EsResult {
    private final long minStart;
    private final long maxEnd;
    private final int callSetId;
    private final int variantSetId;
    private final String referenceName;

    public static EsResult createEsResult(long minStart, long maxEnd, int callSetId, int variantSetId,
        String referenceName) {
      return new EsResult(minStart, maxEnd, callSetId, variantSetId, referenceName);
    }

  }




  public static MinMax getMinMax(Client client){
    val query = QueryBuilders.matchAllQuery();
    val aggs = AggregationBuilders.global("minMaxAgg")
        .subAggregation(AggregationBuilders.max("maxEnd").field("end"))
        .subAggregation(AggregationBuilders.min("minStart").field("start"));
    val aggregations = client.prepareSearch(INDEX_NAME).setQuery(query).addAggregation(aggs).execute().actionGet().getAggregations();
    val internalAggregations = ((InternalGlobal)aggregations.get("minMaxAgg")).getAggregations();
    val minStartTerm = (Min)internalAggregations.get("minStart");
    val maxEndTerm = (Max)internalAggregations.get("maxEnd");
    val minStart = (int)minStartTerm.getValue();
    val maxEnd = (int)maxEndTerm.getValue();
    return createMinMax(minStart, maxEnd);
  }

  public static List<Integer> createCallSetIdList(Client client){
    val query = QueryBuilders.matchAllQuery();
    val aggs = AggregationBuilders.nested("byCalls", "calls")
        .subAggregation(AggregationBuilders.terms("byCallSetId").field("calls.call_set_id"));
    val aggregations = client.prepareSearch(INDEX_NAME).setQuery(query).addAggregation(aggs).execute().actionGet().getAggregations();
    val terms = (InternalNested)aggregations.get("byCalls");
    return null;
  }

  public static List<Integer> createVariantSetIdList(Client client){
    val query = QueryBuilders.matchAllQuery();
    val aggs = AggregationBuilders.nested("byCalls", "calls")
        .subAggregation(AggregationBuilders.terms("byVariantSetId").field("calls.variant_set_id"));
    val aggregations = client.prepareSearch(INDEX_NAME).setQuery(query).addAggregation(aggs).execute().actionGet().getAggregations();
    return null;
  }

  private static VariantService buildVariantService(Client client){
    val variantRepo = new VariantRepository(client);
    val headerRepo = new HeaderRepository(client);
    val callSetRepo = new CallSetRepository(client);
    val variantSetRepo = new VariantSetRepository(client);
    val esVariantConverter = new EsVariantConverterJson();
    val esVariantSetConverter = new EsVariantSetConverterJson();
    val esCallSetConverter = new EsCallSetConverterJson();
    val esCallConverter = new EsConsensusCallConverterJson();
    val esVariantCallPairConverter = new EsVariantCallPairConverterJson(esVariantConverter,
        esCallConverter, esVariantConverter, esCallConverter);

    return new VariantService(variantRepo, headerRepo,
        callSetRepo, variantSetRepo, esVariantSetConverter,
        esCallSetConverter, esVariantCallPairConverter);
  }

  public static void main(String[] args){
    val sampleNum = parseInt(getProperty("num_samples", "100"));
    val variantLength = parseInt(getProperty("variant_length", "10"));
    val seed = parseInt(getProperty("seed", "0"));
    val numEpoch = parseInt(getProperty("num_epoch", "5"));




    log.info("Config: \n{}", ServerConfig.toConfigString());
    try {
      val client = newClient();
      val pageSize = 10;
      val variantService = buildVariantService(client);
      val maxMin = getMinMax(client);
      val startGen = createUniConstrainedRandomIntegerGenerator((int)maxMin.getMinStart(), (int)maxMin.getMaxEnd()-variantLength);
      val variantSetGen = createUniConstrainedRandomIntegerGenerator(0, 3);
      val callSetGen = createUniConstrainedRandomIntegerGenerator(1, 1900);
      val referenceNames = newArrayList("1","2","3","4","5","6","7","8","9");
      val refGen = createUniConstrainedRandomStringGenerator(referenceNames);
      val searchVariantsRequestGenerator = createSVRRandomGenerator(startGen,variantSetGen,callSetGen,refGen,variantLength,pageSize);
      val performanceTest = Performance.builder()
          .numSamples(sampleNum)
          .variantService(variantService)
          .SVRRandomGenerator(searchVariantsRequestGenerator)
          .numEpoch(numEpoch)
          .seed(seed)
          .build();
      performanceTest.run();




//      for (val svrSweeper : svrSweeperList){
//        val startGen = createUniConstrainedRandomIntegerGenerator(svrSweeper.getMinStart(), svrSweeper.getMaxEnd()-variantLength);
//        val variantSetGen = createUniConstrainedRandomIntegerGenerator(0, 3);
//        val callSetGen = createUniConstrainedRandomIntegerGenerator(1, 1900);
//        val referenceNames = newArrayList("1","2","3","4","5","6","7","8","11","12");
//        val refGen = createUniConstrainedRandomStringGenerator(referenceNames);
//
//      }
//
//      val callSetGen = createUniConstrainedRandomIntegerGenerator(1, 1900);
//      val referenceNames = newArrayList("1","2","3","4","5","6","7","8","11","12");
//      val refGen = createUniConstrainedRandomStringGenerator(referenceNames);


//      val searchVariantsRequestGenerator = createSearchVariantsRequestGenerator(startGen,variantSetGen,callSetGen,refGen,10,variantLength);
//      val performanceTest = Performance.builder()
//          .numSamples(sampleNum)
//          .variantService(variantService)
//          .searchVariantsRequestGenerator(searchVariantsRequestGenerator)
//          .seed(0)
//          .build();
//      performanceTest.run();
    } catch (Exception e) {
      log.error("Message[{}] : {}\nStackTrace: {}", e.getClass().getName(), e.getMessage(), e);
    }

  }

  @NonNull VariantService variantService;
  private final int numSamples;
  private final long seed;
  private final int numEpoch;

  @NonNull @Singular  private List<SVRRandomGenerator> SVRRandomGenerators;

  @Override
  @SneakyThrows
  public void run() {
    val writer = new FileWriter("results."+System.currentTimeMillis()+".csv");
    HeaderColumnNameMappingStrategy<Stats> strategy = new HeaderColumnNameMappingStrategy<Stats>();
    strategy.setType(Stats.class);
    StatefulBeanToCsvBuilder<Stats> builder = new StatefulBeanToCsvBuilder<Stats>(writer);
    val bean2csv = (StatefulBeanToCsv<Stats>)builder
        .withMappingStrategy(strategy)
        .withSeparator(CSV_DELIMITER)
        .build();

    log.info("Using seed: {}", seed);
    long totalTime = 0;
    long totalCount = 0;
    for (int epochCount = 0; epochCount < numEpoch ; epochCount++) {
      long epochTime = 0;
      val random = new Random(seed);
      for (val searchVariantRequestGenerator : SVRRandomGenerators) {
        VariantServiceOuterClass.SearchVariantsResponse searchVariantsResponse = null;
        val watch = Stopwatch.createUnstarted();
        int count = 1;
        for (val searchVariantsRequest : searchVariantRequestGenerator.nextRandomList(random, numSamples)) {
          try {
            watch.start();
            searchVariantsResponse = variantService.searchVariants(searchVariantsRequest);
          } catch (Throwable t) {
            log.error("Error runnig variantSearch [{}] -- Message: {}\nStackTrace: {}",
                t.getClass().getName(), t.getMessage(), NEWLINE.join(t.getStackTrace()));

          } finally {
            watch.stop();
          }
          int variantCount = 0;
          try {
            variantCount = searchVariantsResponse.getVariantsCount();
          } catch (Throwable t) {
            log.error("[{}] {}: \nStackTrace:\n{}", t.getClass().getSimpleName(), t.getMessage(),
                NEWLINE.join(t.getStackTrace()));
          }

          val stat = Stats.builder()
              .start(searchVariantsRequest.getStart())
              .end(searchVariantsRequest.getEnd())
              .length(searchVariantRequestGenerator.getVariantLength())
              .variantSetId(searchVariantsRequest.getVariantSetId())
              .callSetIds(SEMICOLON.join(searchVariantsRequest.getCallSetIdsList()))
              .pageSize(searchVariantsRequest.getPageSize())
              .elapsedTimeUs(watch.elapsed(TimeUnit.MICROSECONDS))
              .numResultsReturned(variantCount)
              .referenceName(searchVariantsRequest.getReferenceName())
              .seed(seed)
              .sampleCount(count)
              .sampleTotal(numSamples)
              .build();
          bean2csv.write(stat);

          log.info("VariantCount({} / {}): {}", count, numSamples, variantCount);

          count++;
          epochTime += watch.elapsed(TimeUnit.MICROSECONDS);
          totalTime += epochTime;
          totalCount += numSamples;
          watch.reset();
        }

        val avgEpochTimeUs = epochTime / (double) numSamples;
        val avgEpochTimeMs = epochTime / (double) (numSamples * 1000);
        log.info("NumSamples: [{}], EpochTimeForSamples: {}, Average epoch time (microseconds): {}, Average epoch time (miliseconds): {}",
            numSamples, epochTime,
            avgEpochTimeUs, avgEpochTimeMs);
      }
    }
    writer.close();
    val avgTotalTimeUs = totalTime/ (double) totalCount;
    val avgTotalTimeMs = totalTime/ (double) (totalCount* 1000);
    log.info("NumSamples: [{}], TotalTimeForSamples: {}, Average total time (microseconds): {}, Average total time (miliseconds): {}",
        totalCount, totalTime,
        avgTotalTimeUs, avgTotalTimeMs);
  }

  @Builder
  @Value
  public static class Stats{
    @CsvBindByName private final long start;
    @CsvBindByName private final long end;
    @CsvBindByName private final int length;
    @CsvBindByName private String referenceName;
    @CsvBindByName private final String variantSetId;
    @CsvBindByName private final String callSetIds;
    @CsvBindByName private final int pageSize;
    @CsvBindByName private final long elapsedTimeUs;
    @CsvBindByName private final int numResultsReturned;
    @CsvBindByName private final long seed;
    @CsvBindByName private final int sampleCount;
    @CsvBindByName private final int sampleTotal;

  }

  @Value
  public static class MinMax{

    private final long minStart;
    private final long maxEnd;

    public static MinMax createMinMax(long minStart, long maxEnd) {
      return new MinMax(minStart, maxEnd);
    }

  }

}
