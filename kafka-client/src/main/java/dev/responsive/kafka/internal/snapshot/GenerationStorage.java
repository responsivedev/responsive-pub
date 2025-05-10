package dev.responsive.kafka.internal.snapshot;

import org.apache.kafka.streams.processor.TaskId;

/**
 * Interface that abstracts away how we lookup a task's current generation.
 * For synchronized snapshots we want to do one of the following so we can record a task's
 * generation metadata transactionally alongside the rows with new generation markers when
 * we bump generations:
 * (1) store this information in the offset metadata as part of the transaction that
 *     bumps the generation
 * (2) if/when kafka supports 2pc store this information in another store (like rs3 or
 *     the snapshot store)
 *
 * For simple uncoordinated snapshots we'll support looking up the generation by looking
 * at the snapshot's state in the generation store.
 */
public interface GenerationStorage {
  long lookupGeneration(final TaskId taskId);
}
