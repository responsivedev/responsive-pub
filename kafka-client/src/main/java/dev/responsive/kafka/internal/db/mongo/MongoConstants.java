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

package dev.responsive.kafka.internal.db.mongo;

public class MongoConstants {
  static final String ID_FIELD = "_id";
  static final String SHARD_CMD = "shardCollection";
  static final String SHARD_CMD_KEY_FIELD = "key";
  static final String SHARD_CMD_SCHEME_HASHED = "hashed";
  static final int SHARD_CMD_SCHEME_RANGE = 1;
  static final String SHARD_CMD_CHUNKS_FIELD = "numInitialChunks";
}
