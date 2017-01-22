package org.collaboratory.ga4gh.loader.utils;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Stopwatch;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CounterMonitorOLD {

  private static final int DEFAULT_INTERVAL_SECONDS = 30;
  private static final int DEFAULT_INITAL_COUNT = 0;

  public static CounterMonitorOLD newMonitor(String name, Logger logger, int intervalSeconds) {
    return new CounterMonitorOLD(name, new IntegerCounter(DEFAULT_INITAL_COUNT), Stopwatch.createUnstarted(), logger,
        intervalSeconds);
  }

  public static CounterMonitorOLD newMonitor(String name, Logger logger) {
    return newMonitor(name, logger, DEFAULT_INTERVAL_SECONDS);
  }

  public static CounterMonitorOLD newMonitor(String name, int intervalSeconds) {
    return newMonitor(name, log, intervalSeconds);
  }

  public static CounterMonitorOLD newMonitor(String name) {
    return newMonitor(name, log, DEFAULT_INTERVAL_SECONDS);
  }

  public static void main(String[] args) throws InterruptedException {
    val c = newMonitor("yo", 1);
    c.start();
    Thread.sleep(3000);
    c.stop();
    Thread.sleep(3000);
    log.info("finished wainting, {}", c.getElapsedTimeSeconds());
    c.start();
    Thread.sleep(3000);
    c.stop();
    Thread.sleep(3000);
    log.info("finished wainting again, {}", c.getElapsedTimeSeconds());
    c.start();
    Thread.sleep(3000);
    c.stop();
    log.info("finished wainting again again, {}", c.getElapsedTimeSeconds());
    for (int i = 1; i < 200; i++) {
      c.start();
      Thread.sleep(i);
      c.stop();
      log.info("Done {}", i);
    }

  }

  @NonNull
  private final String name;
  @Getter
  @NonNull
  private final Countable<Integer> counter;

  @NonNull
  private final Stopwatch watch;

  @NonNull
  private final Logger logger;

  private final int intervalSeconds;

  private boolean isRunning = false;
  private long start = 0;
  private boolean threadCreated = false;
  private Thread thread;
  private final Object pauseLock = new Object();

  public synchronized void reset() {
    watch.reset();
    counter.reset();
    start = 0;
    setRunningState(false);
  }

  public synchronized void start() {
    setRunningState(true);
    _start();
  }

  private synchronized void _start() {
    // reset(); // TODO: [rtisma] Take this out once implement Delta feature to track progress between intervals, and
    // not
    // just averages
    setRunningState(true);
    val runnable = new Runnable() {

      @Override
      public void run() {
        while (true) {
          if (!isRunning()) {
            synchronized (pauseLock) {
              log.info("CounterMonitor[{}] stopped running", name);
              break;
            }
            // try {
            // synchronized (pauseLock) {
            // pauseLock.wait();
            // }
            // } catch (InterruptedException e) {
            // log.info("CounterMonitor[{}] started again on threadId {}", name, thread.getId());
            // break;
            // }
          } else {
            val c = counter.getCount();
            val t = watch.elapsed(TimeUnit.SECONDS);
            logger.info("Interval: {}   Count: {}   ElapsedTime: {}   AvgRate(count/sec): {} ",
                name,
                intervalSeconds,
                counter.getCount(),
                t, t == 0 ? 0 : c / t);

            try {
              Thread.sleep(intervalSeconds * 1000);
            } catch (Exception e) {
            }
          }

        }
      }
    };
    thread = new Thread(runnable);
    thread.setName("CounterMonitorThread-" + name + "[" + thread.getId() + "]");
    watch.start();
    thread.start();
  }

  public synchronized void stop() {
    synchronized (pauseLock) {
      watch.stop();
      setRunningState(false);
    }
  }

  public long getElapsedTimeSeconds() {
    return watch.elapsed(TimeUnit.SECONDS);
  }

  private synchronized void setRunningState(boolean isRunning) {
    this.isRunning = isRunning;
  }

  private synchronized boolean isRunning() {
    return isRunning;
  }

}
