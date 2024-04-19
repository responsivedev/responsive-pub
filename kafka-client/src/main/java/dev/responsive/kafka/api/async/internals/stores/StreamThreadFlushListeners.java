/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals.stores;

import dev.responsive.kafka.api.async.internals.AsyncProcessor;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * A class for tracking all of the async flushing stores (eg
 * {@link AsyncFlushingKeyValueStore}) within a givenAsyncProcessor that
 * have been created by this StreamThread, and facilitating the connection
 * between the individual AsyncFlushingStore instances and the
 * {@link AsyncFlushListener}s they need in order to hook into the correct
 * AsyncProcessor instance and flush it during a commit.
 * <p>
 * Longer explanation:
 * This class essentially operates during the creation of a new task through its
 * initialization. It keeps track of information that we only receive in small
 * pieces at different points along the task's journey from creation to initialization.
 * The relevant steps occur in this order:
 *  1. physical processor instance is created (ie ProcessorSupplier#get is called)
 *  2. any state stores connected to that processor are created via StoreBuilder#build
 *  3. the processor is initialized via Processor#init
 *  4. the state store is initialized via StateStore#init
 * <p>
 * At a high level, everything in this class serves a simple need: establishing
 * a connection between an individual AsyncFlushingStore and the specific
 * AsyncProcessor instance that it needs to flush. Unfortunately, this is
 * significantly harder than it would seem, due to the order of operations
 * and slow drip of information such as which StreamTask/partition the store
 * and processor correspond to.
 * The difficulty is this:
 * Even though the ProcessorSupplier has a handle on all the AsyncStoreBuilders,
 * the actual state store instance itself is not yet created when the processor
 * supplier constructs a new processor, so we can't connect the new AsyncProcessor
 * instance to any stores at this time. And even though the AsyncProcessor we
 * want to connect to does exist when the StoreBuilder goes to build the
 * AsyncFlushingStore layer for a newly constructed state store, we have no way
 * of getting a handle on the specific AsyncProcessor that was just created from
 * within the #build method. Even if the AsyncProcessor passed itself to the
 * StoreBuilder and the builder saved a reference to each new processor, we wouldn't
 * know which processor to connect to when #build is called, since the StoreBuilder
 * is used by all StreamThreads across all their StreamTasks. We can only resolve
 * the StreamThread at this point, but not the StreamTask, since neither the
 * AsyncProcessor nor the AsyncFlushingStore have any way to tell which
 * task/partition they are associated with from within their constructors.
 * So, we need to wait until both the processor and the store are initialized,
 * and Streams passes the ProcessorContext into #init from which we can at last
 * extract the partition.
 * Putting it all together, since the processor is initialized first, it will
 * register a callback that triggers flush on itself, along with the partition
 * that flush callback (ie {@link AsyncFlushListener}) is mapped to, with the
 * StoreBuilder. The StoreBuilder itself maintains a map from each StreamThread
 * to the metadata for this store/StoreBuilder, including a map from partition
 * to AsyncFlushListener that it builds up as processors are initialized.
 * Then when the state store is initialized at long last and gets the context
 * that includes the partition information, it can use its stored reference
 * to the metadata object from the StoreBuilder to finally retrieve the
 * AsyncFlushListener that is associated with the same partition.
 * <p>
 * TODO: This is obviously super convoluted so we should investigate whether
 *  there is a better way to connect up the flushing layer with the processor
 *  it needs to flush. This may not be possible without extensive changes to
 *  Kafka Streams itself, but it's worth exploring.
 * <p>
 * Threading notes:
 * -only ever accessed by/on StreamThreads
 * -each StoreBuilder creates exactly one of these for each StreamThread
 * -should only ever be accessed by that particular StreamThread, so no
 *  concurrency precautions are taken within this class
 */
public class StreamThreadFlushListeners {

  private final Logger log;
  private final String streamThreadName;
  private final Map<Integer, FlushListenerConnector> partitionToStoreConnector = new HashMap<>();

  public StreamThreadFlushListeners(
      final String streamThreadName,
      final String storeName
  ) {
    this.streamThreadName = streamThreadName;
    this.log = new LogContext(String.format(
        "stream-thread [%s] %s: ", streamThreadName, storeName
    )).logger(StreamThreadFlushListeners.class);
  }

  /**
   * Use the partitionToStoreConnector map to look up the async flushing store
   * for this partition and StreamThread, and then pass in the {@link AsyncFlushListener}
   * that matches.
   * Called by the {@link AsyncProcessor} from its #init method, after the
   * store has been initialized and registered a connector for its partition
   */
  public void registerListenerForPartition(
      final int partition,
      final AsyncFlushListener listener
  ) {
    final FlushListenerConnector storeConnector = partitionToStoreConnector.remove(partition);
    if (storeConnector == null) {
      log.error("Tried to register the flush listener for this processor with"
                    + "the corresponding async store, but no store for this partition "
                    + "had registered a connector to hook up the listener to the store");
      throw new IllegalStateException("Failed to register new async flush listener "
                                          + "for partition " + partition + " because "
                                          + "no connector exists for that partition");
    }

    storeConnector.registerFlushListenerWithStore(listener);
  }

  /**
   * Remove the registered listener for the given partition without using it, for
   * example if the corresponding task is closed before it goes through initialization
   * and won't be able to retrieve the listener via the usual means.
   * <p>
   * If you want to return the listener after removing, use
   * {@link #registerStoreConnectorForPartition} instead.
   * <p>
   * Note: this method is idempotent and safe to call on a partition for which the
   * listener was already retrieved and removed from the map. This allows the
   * processor to safely unregister partitions when a processor is closed, without
   * having to keep track of whether each of its stores were initialized with a
   * listener before being closed.
   */
  public void unregisterListenerForPartition(
      final int partition
  ) {
    partitionToStoreConnector.remove(partition);
  }

  /**
   * Called by the async flushing store when the store is initialized, which
   * must happen before the processor is initialized and retrieves the connector.
   */
  public void registerStoreConnectorForPartition(
      final int partition,
      final FlushListenerConnector storeConnector
  ) {
    if (partitionToStoreConnector.containsKey(partition)) {
      log.error("Tried to register a new connector for partition {} but one already exists.",
                partition);
      throw new IllegalStateException("Failed to register new store connector for partition "
                                          + partition + " because a connector already exists");
    }
    partitionToStoreConnector.put(partition, storeConnector);
  }

  public String streamThreadName() {
    return streamThreadName;
  }

  @FunctionalInterface
  public interface FlushListenerConnector {

    /**
     * A simple functional interface which should, when invoked, register the
     * flush listener provider with the corresponding async flushing store
     */
    void registerFlushListenerWithStore(final AsyncFlushListener flushListener);
  }

  @FunctionalInterface
  public interface AsyncFlushListener {

    /**
     * A simple runnable that, when executed, will flush all async buffers
     * and perform any output work to finalize pending events (such as issuing
     * their delayed forwards and writes) for a given AsyncProcessor.
     * <p>
     * This method will block until all in-flight events have been completed.
     * This may include simply waiting on the AsyncThreadPool to finish
     * processing all scheduled events and return them to the StreamThread,
     * or actively processing the output records of those records, or a mix
     * of both.
     * <p>
     * When a StreamThread creates a newly-assigned task and builds a new
     * AsyncProcessor instance in the topology, that processor will register
     * a new AsyncFlushListener with the processor's AbstractAsyncStoreBuilder (which
     * is shared by all StreamThreads). The StreamThread will then build any
     * state stores in the processor, at which time the AbstractAsyncStoreBuilder will
     * pass the AsyncFlushListener that corresponds to the current StreamThread,
     * <p>
     * Threading notes:
     * -to be executed by/on the StreamThread only
     * -one instance per physical AsyncProcessor
     *    (ie store per processor per partition per StreamThread)
     *    Important: in case of a zombie, there will two AsyncProcessors that
     *    correspond to the same logical AsyncProcessor and to the same
     *    task/partition. In this case, there will also be two AsyncFlushListeners,
     *    one for each of the two AsyncProcessors owned by different threads (the
     *    zombie and the "real" thread. It's important that an AsyncFlushListener
     *    always point to the same AsyncProcessor it was created for, and should
     *    not be updated/replaced when a zombie is fenced.
     *    This is why they are "per StreamThread" in addition to the rest
     * -while they are technically created for each individual state store
     *  within a processor, in the case of multiple state stores they will
     *  just end up pointing to the same AsyncProcessor and it will be flushed
     *  by each of the stores. Since flushing an emptied AsyncProcessor is a
     *  no-op, this is fine
     */
    void flushBuffers();
  }
}
