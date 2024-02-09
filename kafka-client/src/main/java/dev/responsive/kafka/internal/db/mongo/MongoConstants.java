package dev.responsive.kafka.internal.db.mongo;

public class MongoConstants {
  static final String ID_FIELD = "_id";
  static final String SHARD_CMD = "shardCollection";
  static final String SHARD_CMD_KEY_FIELD = "key";
  static final String SHARD_CMD_SCHEME_HASHED = "hashed";
  static final int SHARD_CMD_SCHEME_RANGE = 1;
  static final String SHARD_CMD_CHUNKS_FIELD = "numInitialChunks";
}
