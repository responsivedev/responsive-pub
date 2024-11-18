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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;

public enum ColumnName {

  // shared partition key column
  PARTITION_KEY("partitionKey", "partitionkey"),
  SEGMENT_START_TIMESTAMP("segmentStartTimestamp", "segmentstarttimestamp"),

  // columns for the data tables
  ROW_TYPE("type", "type"),
  DATA_KEY("key", "datakey", b -> bytes((Bytes) b)),
  DATA_VALUE("value", "value", b -> bytes((byte[]) b)),
  OFFSET("offset", "offset"),
  EPOCH("epoch", "epoch"),
  STREAM_TIME("streamTime", "streamtime"),
  WINDOW_START("windowStart", "windowstart", ts -> timestamp((long) ts)),
  TIMESTAMP("ts", "ts", ts -> timestamp((long) ts)),
  TTL_SECONDS("ttl", "ttl", ttl -> ttlSeconds((int) ttl));

  static final Bytes METADATA_KEY
      = Bytes.wrap("_metadata".getBytes(StandardCharsets.UTF_8));
  static final long METADATA_TS = -1L;
  private final String column;
  private final String bind;
  private final Function<Object, Literal> getLiteral;

  private static Literal bytes(final byte[] b) {
    return QueryBuilder.literal(ByteBuffer.wrap(b));
  }

  private static Literal bytes(final Bytes b) {
    return QueryBuilder.literal(ByteBuffer.wrap(b.get()));
  }

  private static Literal timestamp(final long ts) {
    return QueryBuilder.literal(Instant.ofEpochMilli(ts));
  }

  private static Literal ttlSeconds(final int ttl) {
    return QueryBuilder.literal(ttl);
  }

  ColumnName(final String column, final String bind) {
    this(column, bind, QueryBuilder::literal);
  }

  ColumnName(final String column, final String bind, final Function<Object, Literal> getLiteral) {
    this.column = column;
    this.bind = bind;
    this.getLiteral = getLiteral;
  }

  public String column() {
    return column;
  }

  public String bind() {
    return bind;
  }

  public Literal literal(final Object value) {
    return getLiteral.apply(value);
  }

  public ColumnRelationBuilder<Relation> relation() {
    return Relation.column(column);
  }
}