package dev.responsive.kafka.internal.db.mongo;

import dev.responsive.kafka.api.config.ResponsiveConfig;

public class CollectionCreationOptions {
  private final boolean sharded;
  private final int numChunks;

  CollectionCreationOptions(final boolean sharded, final int numChunks) {
    this.sharded = sharded;
    this.numChunks = numChunks;
  }

  public static CollectionCreationOptions fromConfig(final ResponsiveConfig config) {
    return new CollectionCreationOptions(
        config.getBoolean(ResponsiveConfig.MONGO_COLLECTION_SHARDING_ENABLED_CONFIG),
        config.getInt(ResponsiveConfig.MONGO_COLLECTION_SHARDING_CHUNKS_CONFIG)
    );
  }

  public boolean sharded() {
    return sharded;
  }

  public int numChunks() {
    return numChunks;
  }
}
