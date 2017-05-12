package org.icgc.dcc.ga4gh.server.performance;

import com.google.common.collect.Sets;
import ga4gh.VariantServiceOuterClass.SearchVariantsRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Random;

@Slf4j
@RequiredArgsConstructor
public class SearchVariantsRequestGenerator implements RandomGenerator<SearchVariantsRequest> {

  public static SearchVariantsRequestGenerator createSearchVariantsRequestGenerator(
      RandomGenerator<Integer> startGenerator,
      RandomGenerator<Integer> variantSetIdGenerator,
      RandomGenerator<Integer> callSetIdGenerator,
      RandomGenerator<String> referenceNameGenerator,
      int pageSize, int variantLength) {
    return new SearchVariantsRequestGenerator(startGenerator, variantSetIdGenerator,
        callSetIdGenerator, referenceNameGenerator, pageSize, variantLength);
  }

  private final RandomGenerator<Integer> startGenerator;
  private final RandomGenerator<Integer> variantSetIdGenerator;
  private final RandomGenerator<Integer> callSetIdGenerator;
  private final RandomGenerator<String> referenceNameGenerator;
  private final int variantLength;
  private final int pageSize;
  private static final int MAX_NUM_CALLSET_IDS = 5;

  @Override
  public SearchVariantsRequest nextRandom(Random random){
    val numberOfCallSetIds = random.nextInt(MAX_NUM_CALLSET_IDS) + 1;
    val callSetIds = Sets.<String>newHashSet();

    for (int i = 0; i < numberOfCallSetIds; i++) {
      callSetIds.add(callSetIdGenerator.nextRandom(random).toString());
    }
    val start = startGenerator.nextRandom(random);
    return SearchVariantsRequest.newBuilder()
        .addAllCallSetIds(callSetIds)
        .setReferenceName(referenceNameGenerator.nextRandom(random))
        .setStart(start)
        .setEnd(start + variantLength)
        .setVariantSetId(variantSetIdGenerator.nextRandom(random).toString())
        .setPageSize(pageSize)
        .setPageToken("")
        .build();
  }

}
