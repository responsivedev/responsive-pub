package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Collection;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public class MergeIterator implements KeyValueIterator<Bytes, byte[]> {
  private final PriorityQueue<IteratorEntry> iterators = new PriorityQueue<>();

  public MergeIterator(Collection<KeyValueIterator<Bytes, byte[]>> iterators) {
    for (final var iterator : iterators) {
      this.iterators.add(new IteratorEntry(iterator));
    }
  }

  @Override
  public void close() {

  }

  @Override
  public Bytes peekNextKey() {


    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public KeyValue<Bytes, byte[]> next() {
    final var nextIterator = iterators.poll();
    if (nextIterator == null) {
      throw new IllegalStateException("No more key-values to iterate.");
    } else {
      final var res = nextIterator.advance();


      this.iterators.add(nextIterator);
      return res;
    }
  }

  private static class IteratorEntry implements Comparable<IteratorEntry> {
    private KeyValueIterator<Bytes, byte[]> iterator;
    private Optional<KeyValue<Bytes, byte[]>> next;

    public IteratorEntry(KeyValueIterator<Bytes, byte[]> iterator) {
      this.iterator = iterator;
      this.advance();
    }

    public Optional<KeyValue<Bytes, byte[]>> advance() {
      final var res = this.next;
      if (iterator.hasNext()) {
        this.next = Optional.of(iterator.next());
      } else {
        this.next = Optional.empty();
      }
      return res;
    }

    boolean hasNext() {
      return next.isPresent();
    }

    @Override
    public int compareTo(final IteratorEntry o) {


      return 0;
    }
  }
}
