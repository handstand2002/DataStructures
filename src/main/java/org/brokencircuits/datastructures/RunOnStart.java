package org.brokencircuits.datastructures;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.brokencircuits.datastructures.structure.ConcurrentHashMapWithComputeQueue;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RunOnStart implements Runnable {

  public static ConcurrentHashMapWithComputeQueue<String, Integer> map = new ConcurrentHashMapWithComputeQueue<>();
  public static AtomicInteger lastUpdateNumber = new AtomicInteger(0);

  @Override
  public void run() {

    String securityOne = "SECURITY";
    String securityTwo = "SEC2";
    map.put(securityOne, 0);
    map.put(securityTwo, 10);

    (new Thread(new UpdateRunnerOne(securityOne))).start();
    (new Thread(new UpdateRunnerOne(securityTwo))).start();
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    (new Thread(new UpdateRunnerTwo(securityOne))).start();
    (new Thread(new UpdateRunnerTwo(securityTwo))).start();
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    (new Thread(new QueryRunner(securityOne))).start();
    (new Thread(new QueryRunner(securityTwo))).start();

    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

@Slf4j
@RequiredArgsConstructor
class QueryRunner implements Runnable {

  final String securityId;

  @Override
  public void run() {

    while (true) {
      AtomicLong qty = new AtomicLong(0);
      RunOnStart.map
          .queueCompute(RunOnStart.lastUpdateNumber.incrementAndGet(), "Query Thread", securityId,
              (s, integer) -> {
                log.info("Query thread {}. value: {}", s, integer);
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                log.info("Query operation finished for {}", securityId);
                return integer;
              });
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}

@Slf4j
@RequiredArgsConstructor
class UpdateRunnerOne implements Runnable {

  final String securityId;

  static int threadId = 0;
  private int thisThreadId = ++threadId;

  @Override
  public void run() {
    for (int i = 0; i < 5; i++) {
//      log.info("creating new updateID. Last: {} (Update 1)", RunOnStart.lastUpdateNumber.get());
      RunOnStart.map
          .queueCompute(RunOnStart.lastUpdateNumber.incrementAndGet(),
              "Update Thread 1 sub " + thisThreadId, securityId, (s, integer) -> {
                log.info("Starting atomic update on thread 1");
                try {
                  Thread.sleep(500);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                integer += 1;
                log.info("Finished atomic update on thread 1 - new value: {}", integer);
                return integer;
              });
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}

@Slf4j
@RequiredArgsConstructor
class UpdateRunnerTwo implements Runnable {

  final String securityId;
  static int threadId = 0;
  private int thisThreadId = ++threadId;

  @Override
  public void run() {
//    log.info("creating new updateID. Last: {} (Update 2)", RunOnStart.lastUpdateNumber.get());
    for (int i = 0; i < 5; i++) {
      RunOnStart.map
          .queueCompute(RunOnStart.lastUpdateNumber.incrementAndGet(),
              "Update Thread 2 sub " + thisThreadId, securityId, (s, integer) -> {
                log.info("Starting atomic update on thread 2");
                try {
                  Thread.sleep(500);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                integer += 1;
                log.info("Finished atomic update on thread 2 - new value: {}", integer);
                return integer;
              });
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}