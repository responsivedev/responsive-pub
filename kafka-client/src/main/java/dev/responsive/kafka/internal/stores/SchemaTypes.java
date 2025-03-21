/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;

public class SchemaTypes {

  public enum KVSchema {

    /**
     * A general purpose key value store that supports fencing zombie
     * writers to ensure values are consistent with the latest write.
     */
    KEY_VALUE,

    /**
     * A fact table schema expects all keys to be recorded with only
     * single "fact" values that do not change. It does not support
     * overwriting the value of a record (although no-op updates are
     * not disallowed). Examples of such tables are time-series
     * data and duplicate detection.
     * <p>
     * This schema will perform better than the regular KEY_VALUE
     * but should only be used under conditions described above.
     */
    FACT
  }

  public enum WindowSchema {

    /**
     * A general purpose window store that supports fencing zombie
     * writers to ensure values are consistent with the latest write.
     * Follows regular update/overwrite semantics with exactly one
     * value per window per key.
     * <p>
     * Should be used for all windowed aggregation operators in the DSL
     * (eg count, reduce, aggregate), and generally recommended for any
     * true "time windowed" data in the PAPI.
     */
    WINDOW,

    /**
     * A stream-like window store that supports fencing zombie writers
     * to ensure values are consistent with the latest write.
     * <p>
     * A {@code STREAM} store holds individual events corresponding to
     * a specific timestamp, rather than multi-event per-window partial
     * aggregates like in a typical windowed aggregation.
     * <p>
     * The main difference between this and the {@code WINDOW} store is
     * that the {@code STREAM} store schema supports retaining duplicate
     * events for a given key and timestamp, rather than enforcing
     * update/overwrite semantics.
     * <p>
     * Should only be used in the DSL for stream-stream joins, and is
     * generally recommended for things like timeseries data in the PAPI,
     * or when needing to store multiple values per key/timestamp instead
     * of overwriting the old value.
     */
    STREAM
  }

  public enum SessionSchema {

    /**
     * A general purpose session store that supports fencing zombie
     * writers to ensure values are consistent with the latest write.
     * Follows regular update/overwrite semantics with exactly one
     * value per session per key.
     * <p>
     * Should be used for all session aggregation operators in the DSL
     * (eg count, reduce, aggregate), and generally recommended for any
     * true "session windowed" data in the PAPI.
     */
    SESSION,
  }
}
