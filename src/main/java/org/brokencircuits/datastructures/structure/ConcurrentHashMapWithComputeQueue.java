package org.brokencircuits.datastructures.structure;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConcurrentHashMapWithComputeQueue<K, V> extends ConcurrentHashMap<K, V> {

  private Map<K, KeyStatusAndActionQueue> queuedActionsByKey = new ConcurrentHashMap<>();

  public void queueCompute(int id, String identifier, K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {

    if (remappingFunction == null) {
      throw new NullPointerException("remapping function must not be null");
    }
    queueCompute(id, identifier, key, remappingFunction, v -> {
//      log.info("Finished action with result {}", v);
    });
  }

  public void queueCompute(int id, String identifier, K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction, Consumer<V> callback) {

    if (remappingFunction == null || callback == null) {
      throw new NullPointerException("callback function must not be null");
    }

    KeyStatusAndActionQueue keyStatusAndQueue = queuedActionsByKey.compute(key,
        (k, keyStatusAndActionQueue) -> {
          if (keyStatusAndActionQueue == null) {
            keyStatusAndActionQueue = new KeyStatusAndActionQueue(new AtomicBoolean(false),
                new LinkedBlockingQueue<>());
          }
          return keyStatusAndActionQueue;
        });

    ActionToDoOnKey action = new ActionToDoOnKey(key, remappingFunction, callback, id,
        identifier);
    keyStatusAndQueue.queue.add(action);
    log.info("Queued action on key with externalID: {} identifier: {}", id, identifier);

    if (!keyStatusAndQueue.isBusy.get()) {
      new Thread(() -> {
        while (!keyStatusAndQueue.queue.isEmpty()
            && keyStatusAndQueue.isBusy.compareAndSet(false, true)) {
          queueDoCompute(key, keyStatusAndQueue.queue);
          keyStatusAndQueue.isBusy.set(false);
        }
      }).start();
    }
  }

  public void queueCompute(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction, Consumer<V> callback) {
    queueCompute(-1, "", key, remappingFunction, callback);
  }

  public void queueCompute(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    queueCompute(0, "", key, remappingFunction);
  }

  public boolean keyIsBusy(K key) {
    if (!queuedActionsByKey.containsKey(key)) {
      return false;
    } else {
//      log.info("KeyIsBusy [{}]: {}", key, queuedActionsByKey.get(key));
      KeyStatusAndActionQueue statusAndActionQueue = queuedActionsByKey.get(key);
      return statusAndActionQueue.isBusy.get() || !statusAndActionQueue.queue.isEmpty();
    }
  }

  /**
   * Return the number of actions that have not been executed yet for a particular key. In the case
   * of no actions pending for a key, upon the creation of a new action to be taken the action is
   * immediately put in queue and then removed from the queue when the processing thread is
   * started.
   *
   * @param key key
   * @return number of actions queued for processing
   */
  public int queuedActionsForKey(K key) {

    if (!queuedActionsByKey.containsKey(key)) {
      return 0;
    } else {
      return queuedActionsByKey.get(key).queue.size();
    }
  }

  private void queueDoCompute(K key, Queue<ActionToDoOnKey> queue) {
    ActionToDoOnKey actionToTake = queue.poll();
    actionToTake.callback.accept(compute(key, actionToTake.action));
  }

  @ToString
  @RequiredArgsConstructor
  private class KeyStatusAndActionQueue {

    final AtomicBoolean isBusy;
    final Queue<ActionToDoOnKey> queue;
  }

  @ToString
  @RequiredArgsConstructor
  private class ActionToDoOnKey {

    final K key;
    final BiFunction<? super K, ? super V, ? extends V> action;
    final Consumer<V> callback;
    final int id;
    final String identifier;
  }
}