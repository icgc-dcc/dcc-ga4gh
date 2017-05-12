package org.icgc.dcc.ga4gh.server.performance;

import com.google.common.collect.ImmutableList;
import lombok.val;

import java.util.List;
import java.util.Random;

public interface RandomGenerator<T> {

  T nextRandom(Random random);

  default List<T> nextRandomList(Random random, int num){
    val list = ImmutableList.<T>builder();
    for (int i=0; i<num; i++){
      list.add(nextRandom(random));
    }
    return list.build();
  }

}
