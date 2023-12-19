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

package dev.responsive.kafka.internal.db;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.WindowKeySchema;
import org.junit.jupiter.api.Test;

public class WindowedKeySpecTest {

  private static final Random RANDOM = new Random();
  private static final WindowedKeySpec keySpec = new WindowedKeySpec(w -> w.windowStartMs > 0L);

  @Test
  public void verifyWindowKeySchemaEncoding() {
    final byte[] keyByteArray = new byte[10];
    RANDOM.nextBytes(keyByteArray);

    final long windowStartTimestamp = 11223344L;
    final Bytes changelogRecordKey = WindowKeySchema.toStoreKeyBinary(
        Bytes.wrap(keyByteArray),
        windowStartTimestamp,
        RANDOM.nextInt()
    );

    final ConsumerRecord<byte[], byte[]> changelogRecord =
        new ConsumerRecord<>("topic", 0, 0L, changelogRecordKey.get(), null);

    final WindowedKey windowedKey = keySpec.keyFromRecord(changelogRecord);

    assertThat(windowedKey.windowStartMs, equalTo(windowStartTimestamp));
    assertThat(windowedKey.key, equalTo((Bytes.wrap(keyByteArray))));
  }

}
