package org.icgc.dcc.ga4gh.server.performance;

import com.google.common.base.Stopwatch;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.ga4gh.server.variant.VariantService;

import java.util.List;
import java.util.Random;

import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;

@Builder
@RequiredArgsConstructor
@Slf4j
public class Performance implements Runnable {

  @NonNull VariantService variantService;
  private final int numSamples;
  private final long seed;

  @NonNull @Singular  private List<SearchVariantsRequestGenerator> searchVariantsRequestGenerators;

  @Override
  public void run() {
    val random = new Random(seed);
    log.info("Using seed: {}", seed);
    for (val searchVariantRequestGenerator : searchVariantsRequestGenerators){
      val watch = Stopwatch.createUnstarted();
      for (val searchVariantsRequest : searchVariantRequestGenerator.nextRandomList(random,numSamples)){
        try{
          watch.start();
          variantService.searchVariants(searchVariantsRequest);
        } catch (Throwable t){
          log.error("Error runnig variantSearch [{}] -- Message: {}\nStackTrace: {}",
              t.getClass().getName(),t.getMessage(), NEWLINE.join(t.getStackTrace()) );

        } finally{
          watch.stop();
        }
      }
    }
  }
}
