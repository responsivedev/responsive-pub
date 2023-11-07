/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.db.mongo;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.mongodb.client.MongoClient;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoKVTableTest {

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.MONGO_DB);

  private String name;
  private MongoClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(STORAGE_HOSTNAME_CONFIG);
    client = SessionUtil.connect(mongoConnection, null, null);
  }

  @Test
  public void shouldSucceedWriterWithSameEpoch() throws ExecutionException, InterruptedException {
    // Given:
    final MongoKVTable table = new MongoKVTable(client, name);

    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 100);
    writer.flush();

    // When:
    // use the same writer, which should have the same epoch
    writer.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(true));
  }

  @Test
  public void shouldSucceedWriterWithLargerEpoch() throws ExecutionException, InterruptedException {
    // Given:
    var table = new MongoKVTable(client, name);
    var writerFactory = table.init(0);
    var writer = writerFactory.createWriter(0);
    writer.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 100);
    writer.flush();

    // When:
    // initialize new writer with higher epoch
    table = new MongoKVTable(client, name);
    writerFactory = table.init(0);
    writer = writerFactory.createWriter(0);
    writer.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(true));
  }

  @Test
  public void shouldFenceWriterSmallerEpoch() throws ExecutionException, InterruptedException {
    // Given:
    var table0 = new MongoKVTable(client, name);
    var writerFactory0 = table0.init(0);
    var writer0 = writerFactory0.createWriter(0);

    var table1 = new MongoKVTable(client, name);
    var writerFactory1 = table1.init(0);
    var writer1 = writerFactory1.createWriter(0);

    writer1.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 100);
    writer1.flush();

    // When:
    // initialize new writer with higher epoch
    writer0.insert(Bytes.wrap(new byte[] {1}), new byte[] {1}, 101);
    final CompletionStage<RemoteWriteResult<Integer>> flush = writer0.flush();

    // Then:
    assertThat(flush.toCompletableFuture().get().wasApplied(), is(false));
  }

}