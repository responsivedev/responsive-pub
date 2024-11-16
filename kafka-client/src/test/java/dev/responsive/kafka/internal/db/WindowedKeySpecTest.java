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
