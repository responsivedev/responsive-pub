package dev.responsive.kafka.internal.db.rs3.client.jni;

import dev.responsive.kafka.internal.db.rs3.client.RangeBound;
import dev.responsive.rs3.jni.Bound;
import org.apache.kafka.common.utils.Bytes;

class KVBoundMapper implements RangeBound.Mapper<Bytes, dev.responsive.rs3.jni.Bound> {

  @Override
  public Bound map(RangeBound.InclusiveBound<Bytes> b) {
    return Bound.inclusive(b.key().get());
  }

  @Override
  public Bound map(RangeBound.ExclusiveBound<Bytes> b) {
    return Bound.exclusive(b.key().get());
  }

  @Override
  public Bound map(RangeBound.Unbounded<Bytes> b) {
    return Bound.unbounded();
  }
}
