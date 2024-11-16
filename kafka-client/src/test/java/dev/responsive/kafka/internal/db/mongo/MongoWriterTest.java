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

package dev.responsive.kafka.internal.db.mongo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteTable;
import java.nio.charset.Charset;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MongoWriterTest {
  private static final Integer PARTITION = 1;

  @Mock
  private RemoteTable<Bytes, WriteModel<KVDoc>> table;
  @Mock
  private MongoCollection<KVDoc> collection;
  @Captor
  private ArgumentCaptor<BulkWriteOptions> optionsCaptor;

  private MongoWriter<Bytes, Integer, KVDoc> writer;

  @BeforeEach
  public void setup() {
    writer = new MongoWriter<>(table, PARTITION, PARTITION, () -> collection);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFlushWithUnorderedWrites() {
    // given:
    when(table.insert(anyInt(), any(Bytes.class), any(), anyLong()))
        .thenReturn(mock(WriteModel.class));
    writer.insert(Bytes.wrap(data("foo")), data("bar"), 123L);
    writer.insert(Bytes.wrap(data("baz")), data("boz"), 123L);

    // when:
    writer.flush();

    // then:
    verify(collection).bulkWrite(any(), optionsCaptor.capture());
    assertThat(optionsCaptor.getValue().isOrdered(), is(false));
  }

  private byte[] data(final String v) {
    return v.getBytes(Charset.defaultCharset());
  }
}