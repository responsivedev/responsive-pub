/*
 * Copyright 2024 Responsive Computing, Inc.
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

import dev.responsive.kafka.internal.utils.SessionKey;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.SessionKeySchema;
import org.junit.jupiter.api.Test;

public class SessionKeySpecTest {

  private static final Random RANDOM = new Random();
  private static final SessionKeySpec keySpec = new SessionKeySpec(w -> w.sessionEndMs > 0L);

  @Test
  public void verifySessionKeySpecEncoding() {
    final byte[] keyByteArray = new byte[10];
    RANDOM.nextBytes(keyByteArray);

    final long sessionStartTimestamp = 11223344L;
    final long sessionEndTimestamp = 11225444L;
    final Bytes changelogRecordKey = SessionKeySchema.toBinary(
        Bytes.wrap(keyByteArray),
        sessionStartTimestamp,
        sessionEndTimestamp
    );

    final ConsumerRecord<byte[], byte[]> changelogRecord =
        new ConsumerRecord<>("topic", 0, 0L, changelogRecordKey.get(), null);

    final SessionKey windowedKey = keySpec.keyFromRecord(changelogRecord);

    assertThat(windowedKey.key, equalTo((Bytes.wrap(keyByteArray))));
    assertThat(windowedKey.sessionStartMs, equalTo(sessionStartTimestamp));
    assertThat(windowedKey.sessionEndMs, equalTo(sessionEndTimestamp));
  }

}
