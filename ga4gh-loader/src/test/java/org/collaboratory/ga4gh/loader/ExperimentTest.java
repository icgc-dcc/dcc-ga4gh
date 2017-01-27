package org.collaboratory.ga4gh.loader;

import static com.google.common.base.Stopwatch.createStarted;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToInteger;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToIntegerList;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToString;
import static org.collaboratory.ga4gh.core.SearchHitConverters.convertHitToStringList;
import static org.collaboratory.ga4gh.loader.Factory.newClient;
import static org.collaboratory.ga4gh.loader.Factory.newDocumentWriter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.join.ScoreMode;
import org.collaboratory.ga4gh.loader.model.es.EsCall;
import org.collaboratory.ga4gh.loader.model.es.EsVariant;
import org.collaboratory.ga4gh.loader.test.BaseElasticsearchTest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExperimentTest extends BaseElasticsearchTest {

  private static EsVariant createVariantFromHit(SearchHit hit) {
    val start = convertHitToInteger(hit, "start");
    val end = convertHitToInteger(hit, "end");
    val referenceName = convertHitToString(hit, "reference_name");
    val referenceBases = convertHitToString(hit, "reference_bases");
    val alternateBases = convertHitToStringList(hit, "alternate_bases");
    return EsVariant.builder()
        .start(start)
        .end(end)
        .referenceName(referenceName)
        .referenceBases(referenceBases)
        .alternativeBases(alternateBases)
        .build();
  }

  private static EsCall createCallFromHit(SearchHit hit) {
    val non_reference_alleles = convertHitToIntegerList(hit, "non_reference_alleles");
    val end = convertHitToInteger(hit, "end");
    val referenceName = convertHitToString(hit, "reference_name");
    val referenceBases = convertHitToString(hit, "reference_bases");
    val alternateBases = convertHitToStringList(hit, "alternate_bases");
    return EsCall.builder()

        .build();
  }

  @Test
  public void testNested() {
    log.info("Static Config:\n{}", Config.toConfigString());
    try (val client = newClient();
        val writer = newDocumentWriter(client)) {
      val stopWatch = createStarted();

      val query = constantScoreQuery(
          boolQuery()
              .must(matchAllQuery())
              .must(hasChildQuery("call", matchAllQuery(), ScoreMode.None).innerHit(new InnerHitBuilder())));

      client.prepareClearScroll();
      val size = 2;
      SearchResponse resp = client.prepareSearch("dcc-variants")
          .setTypes("variant")
          .setSize(size)
          .setScroll(TimeValue.timeValueMinutes(3))
          .setQuery(query)
          .get();

      boolean hasHits = resp.getHits().getTotalHits() > 0;
      val variantList = Lists.<EsVariant> newArrayList();
      int count = 0;
      do {
        for (val hit : resp.getHits()) {
          log.info("Hit: {}", hit.getSource());
          variantList.add(createVariantFromHit(hit));
          val callList = Lists.<EsCall> newArrayList();
          // for (val innerHit : hit.getInnerHits().get("call")) {
          //
          // }
        }
        resp = client.prepareSearchScroll(resp.getScrollId())
            .setScroll(TimeValue.timeValueMinutes(3))
            .get();
        hasHits = resp.getHits().getTotalHits() > 0;
        count++;
      } while (hasHits && count < 3);
      log.info("Results: \n{}", variantList);
      stopWatch.stop();
      log.info("LoadTime(min): {}", stopWatch.elapsed(TimeUnit.MINUTES));
      log.info("LoadTime(sec): {}", stopWatch.elapsed(TimeUnit.SECONDS));

    } catch (Exception e) {
      log.error("Exception running: ", e);
    }

  }

}
