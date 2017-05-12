package org.icgc.dcc.ga4gh.server.performance;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.List;
import java.util.Random;

@RequiredArgsConstructor
public class UniConstrainedRandomStringGenerator implements RandomGenerator<String> {

  public static UniConstrainedRandomStringGenerator createUniConstrainedRandomStringGenerator(List<String> strings) {
    return new UniConstrainedRandomStringGenerator(strings);
  }

  @NonNull private final List<String> strings;

  @Override
  public String nextRandom(Random random) {
    val idx = random.nextInt(strings.size());
    return strings.get(idx);
  }

}
