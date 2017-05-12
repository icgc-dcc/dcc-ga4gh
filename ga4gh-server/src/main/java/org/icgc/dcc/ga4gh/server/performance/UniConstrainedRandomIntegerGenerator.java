package org.icgc.dcc.ga4gh.server.performance;

import lombok.Getter;

import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public class UniConstrainedRandomIntegerGenerator implements RandomGenerator<Integer> {

  public static UniConstrainedRandomIntegerGenerator createUniConstrainedRandomIntegerGenerator(int max,
      int min) {
    return new UniConstrainedRandomIntegerGenerator(max, min);
  }

  private final int min;
  @Getter private final int range;

  private UniConstrainedRandomIntegerGenerator(int min, int max) {
    checkArgument(min <= max && min >= 0, "The min must be <= max, and min must be >= 0");
    this.min = min; // inclusive
    this.range = max - min;
  }

  public Integer nextRandom(Random random) {
    return min + random.nextInt(range);
  }

}
