package dev.responsive.kafka.internal.db.rs3;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.internals.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PssRangePartitioner implements PssPartitioner {
  private static final Logger LOG = LoggerFactory.getLogger(PssRangePartitioner.class);

  private final Map<String, Integer> pssByPrefix;
  private final Function<byte[], Integer> hasher;

  private PssRangePartitioner(
      final Map<String, Integer> pssByPrefix,
      final Function<byte[], Integer> hasher
  ) {
    this.pssByPrefix = Objects.requireNonNull(pssByPrefix);
    final Set<Integer> allPss = Set.copyOf(pssByPrefix.values());
    if (allPss.size() != pssByPrefix.size()) {
      throw new IllegalStateException("found duplicate pss: " + pssByPrefix.values());
    }
    this.hasher = Objects.requireNonNull(hasher);
  }

  static PssRangePartitioner create(final List<String> pssBitPrefixes) {
    return create(pssBitPrefixes, PssRangePartitioner::murmur3);
  }

  static PssRangePartitioner create(
      final List<String> pssBitPrefixes,
      final Function<byte[], Integer> hasher
  ) {
    return new PssRangePartitioner(
        pssBitPrefixes.stream()
            .collect(Collectors.toMap(
                prefix -> prefix,
                PssRangePartitioner::prefixToPssId
            )),
        hasher
    );
  }

  @Override
  public int pss(byte[] key, int partition) {
    final String hashedBits = hashedBits(key);
    for (final Map.Entry<String, Integer> entry : pssByPrefix.entrySet()) {
      if (hashedBits.startsWith(entry.getKey())) {
        return entry.getValue();
      }
    }
    LOG.error("no pss found for hashed bits {}", hashedBits);
    throw new IllegalStateException("no pss found for hashed bits " + hashedBits);
  }

  @Override
  public List<Integer> allPss() {
    return List.copyOf(pssByPrefix.values());
  }

  private String hashedBits(final byte[] key) {
    final String noLeadingZeros = Integer.toBinaryString(hasher.apply(key));
    if (noLeadingZeros.length() == Integer.SIZE) {
      return noLeadingZeros;
    }
    final StringBuilder hashedBits = new StringBuilder(Integer.SIZE);
    final char[] zeros = new char[Integer.SIZE - noLeadingZeros.length()];
    Arrays.fill(zeros, '0');
    hashedBits.append(zeros);
    hashedBits.append(noLeadingZeros);
    return hashedBits.toString();
  }

  private static int murmur3(final byte[] key) {
    return Murmur3.hash32(key);
  }

  private static int prefixToPssId(final String prefix) {
    final String withLeadingBit = "1" + prefix;
    return Integer.parseUnsignedInt(withLeadingBit, 2);
  }
}
