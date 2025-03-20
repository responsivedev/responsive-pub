package dev.responsive.kafka.internal.db.rs3.client;

public interface RangeBound {

  <T> T map(Mapper<T> mapper);

  static Unbounded unbounded() {
    return Unbounded.INSTANCE;
  }

  static InclusiveBound inclusive(byte[] key) {
    return new InclusiveBound(key);
  }

  static ExclusiveBound exclusive(byte[] key) {
    return new ExclusiveBound(key);
  }


  class InclusiveBound implements RangeBound {
    private final byte[] key;

    public InclusiveBound(final byte[] key) {
      this.key = key;
    }

    public byte[] key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  class ExclusiveBound implements RangeBound {
    private final byte[] key;

    public ExclusiveBound(final byte[] key) {
      this.key = key;
    }

    public byte[] key() {
      return key;
    }

    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  class Unbounded implements RangeBound {
    private static final Unbounded INSTANCE = new Unbounded();
    @Override
    public <T> T map(final Mapper<T> mapper) {
      return mapper.map(this);
    }
  }

  interface Mapper<T> {
    T map(InclusiveBound b);
    T map(ExclusiveBound b);
    T map(Unbounded b);
  }


}
