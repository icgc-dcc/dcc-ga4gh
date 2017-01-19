package org.collaboratory.ga4gh.loader.utils;

import org.slf4j.Logger;

import htsjdk.samtools.util.StopWatch;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CounterMonitor {

  private static final int DEFAULT_INTERVAL_SECONDS = 30;
  private static final int DEFAULT_INITAL_COUNT = 0;

  public static CounterMonitor newMonitor(String name, Logger logger, int intervalSeconds) {
    return new CounterMonitor(name, new Counter(DEFAULT_INITAL_COUNT), new StopWatch(), logger, intervalSeconds);
  }

  public static CounterMonitor newMonitor(String name, Logger logger) {
    return newMonitor(name, logger, DEFAULT_INTERVAL_SECONDS);
  }

  public static CounterMonitor newMonitor(String name, int intervalSeconds) {
    return newMonitor(name, log, intervalSeconds);
  }

  public static CounterMonitor newMonitor(String name) {
    return newMonitor(name, log, DEFAULT_INTERVAL_SECONDS);

  }

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
  private long start = 0;

  public void start() {
    watch.reset();
    start = 0;
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
            if (!isRunning) {
              log.info("CounterMonitor[{}] stopped running", name);
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

  public long getElapsedTimeSeconds() {
    return watch.getElapsedTimeSecs();
  }

  private synchronized void setRunningState(boolean isRunning) {
    this.isRunning = isRunning;
  }

}
