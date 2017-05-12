package org.icgc.dcc.ga4gh.loader.utils.counting;

import com.google.common.base.Stopwatch;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.common.core.util.Formats;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static org.icgc.dcc.ga4gh.loader.utils.counting.LongCounter.createLongCounter;

@Slf4j
public class CounterMonitor implements Counter<Long> {

  private static final long DEFAULT_INITAL_COUNT = 0;

  private final String name;

  private final Counter<Long> counter;

  private final Stopwatch watch;

  private final Logger logger;

  private final long countInterval;

  @Getter
  private boolean isRunning = false;

  private long previousCount;
  private float previousTime;

  public static CounterMonitor createCounterMonitor(String name, Logger logger, long intervalCount, long initCount) {
    return new CounterMonitor(name, createLongCounter(initCount), Stopwatch.createUnstarted(), logger,
        intervalCount);
  }

  public static CounterMonitor createCounterMonitor(String name, Logger logger, int intervalCount) {
    return createCounterMonitor(name, logger, intervalCount, DEFAULT_INITAL_COUNT);
  }

  public static CounterMonitor createCounterMonitor(String name, long intervalCount, long initCount) {
    return createCounterMonitor(name, log, intervalCount, initCount);
  }

  public static CounterMonitor createCounterMonitor(String name, int intervalCount) {
    return createCounterMonitor(name, log, intervalCount, DEFAULT_INITAL_COUNT);
  }

  public CounterMonitor(String name, Counter<Long> counter, Stopwatch watch, Logger logger, long countInterval) {
    this.name = name;
    this.counter = counter;
    this.watch = watch;
    this.logger = logger;
    this.countInterval = countInterval;
    // Init
    reset();
  }

  public void displaySummary() {
    log.info("[{}] SUMMARY: {}", name, toString());
  }

  @Override
  public void reset() {
    watch.reset();
    counter.reset();
    setRunningState(false);
    previousCount = counter.getCount();
    previousTime = getElapsedTimeSeconds();
  }

  public void start() {
    if (!isRunning()){
      setRunningState(true);
      watch.start();
    }
  }

  public void stop() {
    if (isRunning()){
      watch.stop();
      setRunningState(false);
    }
  }

  public float getElapsedTimeSeconds() {
    return getElapsedTimeMicro() / 1000000;
  }

  public float getElapsedTimeMili() {
    return getElapsedTimeMicro() / 1000;
  }

  public float getElapsedTimeMicro() {
    return watch.elapsed(TimeUnit.MICROSECONDS);
  }

  private void setRunningState(boolean isRunning) {
    this.isRunning = isRunning;
  }

  private void monitor() {
    if (isRunning()) {
      val currentCount = counter.getCount();
      val currentIntervalCount = currentCount - previousCount;
      if (currentIntervalCount >= countInterval) {
        val totalTime = getElapsedTimeSeconds();
        val intervalElapsedTime = totalTime - previousTime;
        val instRate = getInstRate();
        val avgRate = getAvgRate();
        logger.info(
            "[CounterMonitor-{}] -- CountInterval: {}   Count: {}   TotalElapsedTime(s): {}   IntervalElapsedTime(s): {}   InstantaeousRate(counter/s): {}  AvgRate(counter/s): {}",
            name,
            countInterval,
            currentCount,
            totalTime,
            intervalElapsedTime,
            instRate,
            avgRate);
        previousCount = currentCount;
        previousTime = totalTime;
      }
    }
  }

  public String getAvgRate() {
    return Formats.formatRate(counter.getCount(), watch);
  }

  public long getInstCount(){
    return counter.getCount()-previousCount;
  }

  public String getInstRate() {
    val currentIntervalCount = getInstCount();
    val intervalElapsedTime = getElapsedTimeSeconds() - previousTime;
    val rate = intervalElapsedTime == 0 ? 0 : currentIntervalCount / intervalElapsedTime;
    return Formats.formatRate(rate);
  }

  @Override
  public Long incr() {
    counter.incr();
    monitor();
    return counter.getCount();
  }

  @Override
  public Long incr(Long amount) {
    counter.incr(amount);
    monitor();
    return counter.getCount();
  }

  @Override
  public Long getCount() {
    return counter.getCount();
  }

  @Override
  public String toString() {
    val currentCount = counter.getCount();
    val instCount = getInstCount();
    val totalTime = getElapsedTimeSeconds();
    val intervalElapsedTime = totalTime - previousTime;
    val instRate = getInstRate();
    val avgRate = getAvgRate();
    return String.format(
        "[CounterMonitor-%s] -- CountInterval: %s   Count: %s   InstCount: %s  TotalElapsedTime(s): %s   IntervalElapsedTime(s): %s   InstRate(counter/sec): %s  AvgRate(counter/sec): %s",
        name,
        countInterval,
        currentCount,
        instCount,
        totalTime,
        intervalElapsedTime,
        instRate,
        avgRate);
  }
}
