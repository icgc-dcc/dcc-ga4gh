package org.collaboratory.ga4gh.loader.utils;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Stopwatch;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CounterMonitor implements Countable<Integer> {

  private static final int DEFAULT_INTERVAL_SECONDS = 30;
  private static final int DEFAULT_INITAL_COUNT = 0;

  public static CounterMonitor newMonitor(String name, Logger logger, int intervalSeconds) {
    return new CounterMonitor(name, new IntegerCounter(DEFAULT_INITAL_COUNT), Stopwatch.createUnstarted(), logger,
        intervalSeconds);
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

  public CounterMonitor(String name, Countable<Integer> counter, Stopwatch watch, Logger logger, int countInterval) {
    this.name = name;
    this.counter = counter;
    this.watch = watch;
    this.logger = logger;
    this.countInterval = countInterval;
    // Init
    this.previousCount = counter.getCount();
    this.previousTime = watch.elapsed(TimeUnit.SECONDS);
  }

  public static void main(String[] args) throws InterruptedException {
    val c = newMonitor("yo", 2);
    c.start();
    for (int i = 0; i < 10; i++) {
      c.incr();
      Thread.sleep(1000);
    }
    c.stop();

  }

  private final String name;

  private final Countable<Integer> counter;

  private final Stopwatch watch;

  private final Logger logger;

  private final int countInterval;

  @Getter
  private boolean isRunning = false;

  private int previousCount;
  private long previousTime;

  @Override
  public void reset() {
    watch.reset();
    counter.reset();
    setRunningState(false);
  }

  public void start() {
    setRunningState(true);
    watch.start();
    logger.info("Started CounterMonitor-{}", name);
  }

  public void stop() {
    watch.stop();
    setRunningState(false);
    logger.info("Stopped CounterMonitor-{}", name);
  }

  public long getElapsedTimeSeconds() {
    return watch.elapsed(TimeUnit.SECONDS);
  }

  private void setRunningState(boolean isRunning) {
    this.isRunning = isRunning;
  }

  private void monitor() {
    if (isRunning()) {
      val currentCount = counter.getCount();
      val currentIntervalCount = currentCount - previousCount;
      if (currentIntervalCount == countInterval) {
        val totalTime = getElapsedTimeSeconds();
        val intervalElapsedTime = totalTime - previousTime;
        val instRate = getInstRate();
        val avgRate = getAvgRate();
        logger.info(
            "[CounterMonitor-{}] -- CountInterval: {}   Count: {}   TotalElapsedTime: {}   IntervalElapsedTime: {}   InstantaeousRate: {}  AvgRate: {}",
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

  public long getAvgRate() {
    val t = getElapsedTimeSeconds();
    return t == 0 ? 0 : counter.getCount() / t;
  }

  public long getInstRate() {
    val currentIntervalCount = counter.getCount() - previousCount;
    val intervalElapsedTime = getElapsedTimeSeconds() - previousTime;
    return intervalElapsedTime == 0 ? 0 : currentIntervalCount / intervalElapsedTime;
  }

  @Override
  public void incr() {
    counter.incr();
    monitor();
  }

  @Override
  public void incr(Integer amount) {
    counter.incr(amount);
    monitor();
  }

  @Override
  public Integer getCount() {
    return counter.getCount();
  }

  @Override
  public String toString() {
    val currentCount = counter.getCount();
    val totalTime = getElapsedTimeSeconds();
    val intervalElapsedTime = totalTime - previousTime;
    val instRate = getInstRate();
    val avgRate = getAvgRate();
    return String.format(
        "[CounterMonitor-%s] -- CountInterval: %s   Count: %s   TotalElapsedTime(s): %s   IntervalElapsedTime(s): %s   InstantaeousRate(count/sec): %s  AvgRate(count/sec): %s",
        name,
        countInterval,
        currentCount,
        totalTime,
        intervalElapsedTime,
        instRate,
        avgRate);
  }
}
