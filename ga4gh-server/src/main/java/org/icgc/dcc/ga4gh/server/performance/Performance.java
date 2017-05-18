package org.icgc.dcc.ga4gh.server.performance;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsCallSetConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantCallPairConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantConverterJson;
import org.icgc.dcc.ga4gh.common.model.converters.EsVariantSetConverterJson;
import org.icgc.dcc.ga4gh.server.config.ServerConfig;
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
import static org.icgc.dcc.ga4gh.server.performance.SearchRequestSweepIterator.createSearchRequestSweepIterator;
import static org.icgc.dcc.ga4gh.server.performance.SearchVariantsRequestGenerator.createSearchVariantsRequestGenerator;
import static org.icgc.dcc.ga4gh.server.performance.SubSearchVariantRequestIterator.createSubSearchVariantRequestIterator;
import static org.icgc.dcc.ga4gh.server.performance.UniConstrainedRandomIntegerGenerator.createUniConstrainedRandomIntegerGenerator;
import static org.icgc.dcc.ga4gh.server.performance.UniConstrainedRandomStringGenerator.createUniConstrainedRandomStringGenerator;

@Builder
@RequiredArgsConstructor
@Slf4j
public class Performance implements Runnable {
  private final static char CSV_DELIMITER = ',';

  public static List<SubSearchVariantRequestIterator> createList(Client client, int variantLength){
    val query = QueryBuilders.matchAllQuery();
    val aggs = AggregationBuilders.terms("byReferenceName").field("reference_name")
        .subAggregation(AggregationBuilders.max("maxEnd").field("end"))
        .subAggregation(AggregationBuilders.min("minStart").field("start"));
    val aggregations = client.prepareSearch(INDEX_NAME).setQuery(query).addAggregation(aggs).execute().actionGet().getAggregations();
    val terms = (Terms)aggregations.get("byReferenceName");

    val list = ImmutableList.<SubSearchVariantRequestIterator>builder();
    for (Terms.Bucket bucket : terms.getBuckets()){
      val referenceName = bucket.getKeyAsString();
      val minStartTerm = (Min)bucket.getAggregations().get("minStart");
      val minStart = (int)minStartTerm.getValue();

      val maxEndTerm = (Max)bucket.getAggregations().get("maxEnd");
      val maxEnd = (int)maxEndTerm.getValue();
      val it = createSubSearchVariantRequestIterator(referenceName,minStart,maxEnd,variantLength);
      list.add(it);
    }
    return list.build();
  }

  public static void main(String[] args){
    val sampleNum = parseInt(getProperty("num_samples", "100"));
    val variantLength = parseInt(getProperty("variant_length", "10"));

    val subSearchVariantReqIterator1 = createSubSearchVariantRequestIterator("1", 0, 100000, 10);
    val subSearchVariantReqIterator2 = createSubSearchVariantRequestIterator("2", 0, 100000, 10);
    val searchRequestSweepIterator = createSearchRequestSweepIterator(
        newArrayList(subSearchVariantReqIterator1,subSearchVariantReqIterator2));

    int countt = 0;
    while(searchRequestSweepIterator.hasNext()){
      val s = searchRequestSweepIterator.next();
     countt++;
    }


    log.info("Config: \n{}", ServerConfig.toConfigString());
    try {
      val client = newClient();

      val subSearchVariantRequestIteratorList = createList(client,variantLength);
      val searchVariantRequestSweepIterator = createSearchRequestSweepIterator(subSearchVariantRequestIteratorList);
      int count = searchVariantRequestSweepIterator.getSize();

      val variantRepo = new VariantRepository(client);
      val headerRepo = new HeaderRepository(client);
      val callSetRepo = new CallSetRepository(client);
      val variantSetRepo = new VariantSetRepository(client);
      val esVariantConverter = new EsVariantConverterJson();
      val esVariantSetConverter = new EsVariantSetConverterJson();
      val esCallSetConverter = new EsCallSetConverterJson();
      val esCallConverter = new EsCallConverterJson();
      val esVariantCallPairConverter = new EsVariantCallPairConverterJson(esVariantConverter
          , esCallConverter, esVariantConverter, esCallConverter);

      val startGen = createUniConstrainedRandomIntegerGenerator(13,249240613-variantLength);
      val variantSetGen = createUniConstrainedRandomIntegerGenerator(0, 19);
      val callSetGen = createUniConstrainedRandomIntegerGenerator(1, 1900);
      val referenceNames = newArrayList("1","2","3","4","5","6","7","8","11","12");
      val refGen = createUniConstrainedRandomStringGenerator(referenceNames);


      val searchVariantsRequestGenerator = createSearchVariantsRequestGenerator(startGen,variantSetGen,callSetGen,refGen,10,variantLength);
      val variantService =
          new VariantService(variantRepo, headerRepo, callSetRepo, variantSetRepo, esVariantSetConverter, esCallSetConverter, esVariantCallPairConverter);
      val performanceTest = Performance.builder()
          .numSamples(sampleNum)
          .variantService(variantService)
          .searchVariantsRequestGenerator(searchVariantsRequestGenerator)
          .seed(0)
          .build();
      performanceTest.run();
    } catch (Exception e) {
      log.error("Message[{}] : {}\nStackTrace: {}", e.getClass().getName(), e.getMessage(), e);
    }

  }

  @NonNull VariantService variantService;
  private final int numSamples;
  private final long seed;

  @NonNull @Singular  private List<SearchVariantsRequestGenerator> searchVariantsRequestGenerators;

  @Override
  @SneakyThrows
  public void run() {
    val random = new Random(seed);
    val writer = new FileWriter("results."+System.currentTimeMillis()+".csv");
    HeaderColumnNameMappingStrategy<Stats> strategy = new HeaderColumnNameMappingStrategy<Stats>();
    strategy.setType(Stats.class);
    StatefulBeanToCsvBuilder<Stats> builder = new StatefulBeanToCsvBuilder<Stats>(writer);
    val bean2csv = (StatefulBeanToCsv<Stats>)builder
        .withMappingStrategy(strategy)
        .withSeparator(CSV_DELIMITER)
        .build();

    log.info("Query");
    val q = QueryBuilders.matchAllQuery();
    val d = AggregationBuilders.terms("reference_name");





    log.info("Using seed: {}", seed);
    for (val searchVariantRequestGenerator : searchVariantsRequestGenerators){
      VariantServiceOuterClass.SearchVariantsResponse searchVariantsResponse =null;
      val watch = Stopwatch.createUnstarted();
      int count = 1;
      for (val searchVariantsRequest : searchVariantRequestGenerator.nextRandomList(random,numSamples)){
        try{
          watch.start();
          searchVariantsResponse= variantService.searchVariants(searchVariantsRequest);
        } catch (Throwable t){
          log.error("Error runnig variantSearch [{}] -- Message: {}\nStackTrace: {}",
              t.getClass().getName(),t.getMessage(), NEWLINE.join(t.getStackTrace()) );

        } finally{
          watch.stop();
        }
        int variantCount = 0;
        try{
          variantCount = searchVariantsResponse.getVariantsCount();
        } catch (Throwable t){
          log.error("[{}] {}: \nStackTrace:\n{}", t.getClass().getSimpleName(), t.getMessage(), NEWLINE.join(t.getStackTrace()));
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

        log.info("VariantCount({} / {}): {}",count, numSamples,variantCount);

        count++;
        watch.reset();


      }

    }
    writer.close();
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
}
