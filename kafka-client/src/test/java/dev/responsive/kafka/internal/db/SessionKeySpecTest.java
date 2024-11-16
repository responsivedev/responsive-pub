/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
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
  public void shouldDecodeSessionKeyProperly() {
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
