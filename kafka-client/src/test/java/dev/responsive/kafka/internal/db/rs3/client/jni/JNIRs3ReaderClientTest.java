package dev.responsive.kafka.internal.db.rs3.client.jni;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import dev.responsive.kafka.internal.db.rs3.client.Range;
import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.jni.Bound;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JNIRs3ReaderClientTest {
  @Mock
  private dev.responsive.rs3.jni.RS3Reader reader;
  @Mock
  private dev.responsive.rs3.jni.RS3Iterator<byte[], byte[]> kvIterator;
  @Captor
  private ArgumentCaptor<dev.responsive.rs3.jni.Bound> boundCaptor;

  private JNIRs3ReaderClient client;

  @BeforeEach
  public void setup() {
    client = new JNIRs3ReaderClient(reader);
  }

  @Test
  public void shouldIterateUsingJNIIterator() {
    // given:
    when(reader.kvGetRange(any(), anyInt(), anyInt(), any(), any())).thenReturn(kvIterator);
    when(kvIterator.next())
        .thenReturn(Optional.of(new dev.responsive.rs3.jni.RS3Row<>(bytes("foo"), bytes("bar"))))
        .thenReturn(Optional.of(new dev.responsive.rs3.jni.RS3Row<>(bytes("biz"), bytes("baz"))))
        .thenReturn(Optional.empty());

    // when:
    final var iterator = client.range(
        UUID.randomUUID(),
        new LssId(0),
        0,
        Optional.empty(),
        Range.unbounded()
    );

    // then:
    assertThat(iterator.hasNext(), is(true));
    var next = iterator.next();
    assertThat(next.key, is(Bytes.wrap(bytes("foo"))));
    assertThat(Bytes.wrap(next.value), is(Bytes.wrap(bytes("bar"))));
    assertThat(iterator.hasNext(), is(true));
    next = iterator.next();
    assertThat(next.key, is(Bytes.wrap(bytes("biz"))));
    assertThat(Bytes.wrap(next.value), is(Bytes.wrap(bytes("baz"))));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }

  @Test
  public void shouldMapInclusiveBound() {
    // given:
    when(reader.kvGetRange(any(), anyInt(), anyInt(), any(), any())).thenReturn(kvIterator);

    // when:
    final var iterator = client.range(
        UUID.randomUUID(),
        new LssId(0),
        0,
        Optional.empty(),
        new Range<>(
            RangeBound.inclusive(Bytes.wrap(bytes("aaa"))),
            RangeBound.inclusive(Bytes.wrap(bytes("bbb")))
        )
    );

    // then:
    verify(reader).kvGetRange(any(), anyInt(), anyInt(), boundCaptor.capture(), any());
    final var bound = boundCaptor.getValue();
    assertThat(bound.type(), is(Bound.BoundType.Inclusive));
    assertThat(Arrays.equals(bytes("aaa"), bound.bound()), is(true));
    iterator.close();
  }

  @Test
  public void shouldMapExclusiveBound() {
    // given:
    when(reader.kvGetRange(any(), anyInt(), anyInt(), any(), any())).thenReturn(kvIterator);

    // when:
    final var iterator = client.range(
        UUID.randomUUID(),
        new LssId(0),
        0,
        Optional.empty(),
        new Range<>(
            RangeBound.exclusive(Bytes.wrap(bytes("aaa"))),
            RangeBound.inclusive(Bytes.wrap(bytes("bbb")))
        )
    );

    // then:
    verify(reader).kvGetRange(any(), anyInt(), anyInt(), boundCaptor.capture(), any());
    final var bound = boundCaptor.getValue();
    assertThat(bound.type(), is(Bound.BoundType.Exclusive));
    assertThat(Arrays.equals(bytes("aaa"), bound.bound()), is(true));
    iterator.close();
  }

  @Test
  public void shouldMapUnboundedBound() {
    // given:
    when(reader.kvGetRange(any(), anyInt(), anyInt(), any(), any())).thenReturn(kvIterator);

    // when:
    final var iterator = client.range(
        UUID.randomUUID(),
        new LssId(0),
        0,
        Optional.empty(),
        Range.unbounded()
    );

    // then:
    verify(reader).kvGetRange(any(), anyInt(), anyInt(), boundCaptor.capture(), any());
    final var bound = boundCaptor.getValue();
    assertThat(bound.type(), is(Bound.BoundType.Unbounded));
    iterator.close();
  }

  private byte[] bytes(final String str) {
    return str.getBytes(Charset.defaultCharset());
  }
}