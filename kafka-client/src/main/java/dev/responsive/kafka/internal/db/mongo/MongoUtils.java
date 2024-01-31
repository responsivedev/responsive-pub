package dev.responsive.kafka.internal.db.mongo;

import static dev.responsive.kafka.internal.db.mongo.MongoConstants.ID_FIELD;
import static dev.responsive.kafka.internal.db.mongo.MongoConstants.SHARD_CMD;
import static dev.responsive.kafka.internal.db.mongo.MongoConstants.SHARD_CMD_KEY_FIELD;
import static dev.responsive.kafka.internal.db.mongo.MongoConstants.SHARD_CMD_SCHEME_HASHED;
import static dev.responsive.kafka.internal.db.mongo.MongoConstants.SHARD_CMD_SCHEME_RANGE;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MongoUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MongoUtils.class);

  private MongoUtils() {
  }

  static <T> MongoCollection<T> createShardedCollection(
      final String collectionName,
      final Class<T> documentClass,
      final MongoDatabase database,
      final MongoDatabase adminDatabase
  ) {
    final MongoCollection<T> collection = database.getCollection(
        collectionName,
        documentClass
    );
    final BasicDBObject shardKey = new BasicDBObject(ID_FIELD, SHARD_CMD_SCHEME_HASHED);
    final var shardCmd = new BasicDBObject(
        SHARD_CMD,
        String.join(".", database.getName(), collectionName)
    );
    shardCmd.put(SHARD_CMD_KEY_FIELD, shardKey);
    shardCmd.put("numInitialChunks", 4);
    LOG.info("issue cmd {}", shardCmd.toJson());
    final var result = adminDatabase.runCommand(shardCmd);
    LOG.info("shardCollection result: {}", result.toJson());
    return collection;
  }
}
