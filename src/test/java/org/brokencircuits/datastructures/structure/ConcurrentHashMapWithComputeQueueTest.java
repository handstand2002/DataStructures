package org.brokencircuits.datastructures.structure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ConcurrentHashMapWithComputeQueueTest {

  private ConcurrentHashMapWithComputeQueue<String, Integer> map;

  @Before
  public void setUp() {
    map = new ConcurrentHashMapWithComputeQueue<>();
  }

  @Test
  @SneakyThrows
  public void concurrentActionsOnDifferentKeysNoCallback() {

    String keyOne = "KeyOne";
    String keyTwo = "KeyTwo";
    String keyThree = "KeyThree";
    int initValueOne = 0;
    int initValueTwo = 100;
    int initValueThree = 10000;
    int processingTimeMs = 1000;
    map.put(keyOne, initValueOne);
    map.put(keyTwo, initValueTwo);
    map.put(keyThree, initValueThree);

    long startProcessingTime = Instant.now().toEpochMilli();
    map.queueCompute(keyOne, (s, integer) -> {
      try {
        Thread.sleep(processingTimeMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return integer + 1;
    });

    map.queueCompute(keyTwo, (s, integer) -> {
      try {
        Thread.sleep(processingTimeMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return integer + 1;
    });

    map.queueCompute(keyThree, (s, integer) -> {
      try {
        Thread.sleep(processingTimeMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return integer + 1;
    });

    // Verify both keys are busy and that unused key is not
    assertTrue(map.keyIsBusy(keyOne));
    assertTrue(map.keyIsBusy(keyTwo));
    assertTrue(map.keyIsBusy(keyThree));

    // wait until all processing on keys is done
    while (map.keyIsBusy(keyOne) && map.keyIsBusy(keyTwo) && map.keyIsBusy(keyThree)) {
    }

    long actualProcessingTime = Instant.now().toEpochMilli() - startProcessingTime;
    log.info("Took {}ms to finish all processing", actualProcessingTime);

    // verify processing time was within 20% of expected
    assertEquals(processingTimeMs, actualProcessingTime, processingTimeMs * .2);
    assertEquals(initValueOne + 1, (int) map.computeIfPresent(keyOne, (s, integer) -> integer));
    assertEquals(initValueTwo + 1, (int) map.computeIfPresent(keyTwo, (s, integer) -> integer));
    assertEquals(initValueThree + 1, (int) map.computeIfPresent(keyThree, (s, integer) -> integer));
  }

}