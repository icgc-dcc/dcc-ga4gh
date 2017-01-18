package org.collaboratory.ga4gh.loader.utils;

import org.slf4j.Logger;

import htsjdk.samtools.util.StopWatch;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
public class CounterMonitor {

  @NonNull
  private final String name;
  @Getter
  @NonNull
  private final Counter counter;

  @NonNull
  private final StopWatch watch;

  @NonNull
  private final Logger logger;

  private final int intervalSeconds;

  private boolean isRunning = false;

  public void start() {
    watch.reset();
    if (!isRunning) {
      setRunningState(true);
      val runnable = new Runnable() {

        @Override
        public void run() {
          while (isRunning) {

            val c = counter.getCount();
            val t = watch.getElapsedTimeSecs();
            logger.info("CounterMonitor[{}] ==> Interval: {}   Count: {}   ElapsedTime: {}   AvgRate(count/sec): {} ",
                name,
                intervalSeconds,
                counter.getCount(),
                watch.getElapsedTimeSecs(), t == 0 ? 0 : c / t);

            try {
              Thread.sleep(intervalSeconds * 1000);
            } catch (Exception e) {

            }

          }
        }
      };
      Thread t = new Thread(runnable);
      watch.start();
      t.start();
    }
  }

  public void stop() {
    setRunningState(false);
    watch.stop();
  }

  private synchronized void setRunningState(boolean isRunning) {
    this.isRunning = isRunning;
  }

}
