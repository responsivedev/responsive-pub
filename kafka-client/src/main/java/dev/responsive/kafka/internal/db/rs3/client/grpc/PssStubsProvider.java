package dev.responsive.kafka.internal.db.rs3.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.kafka.internal.db.rs3.client.grpc.middleware.PssHeadersInterceptor;
import dev.responsive.rs3.RS3Grpc;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PssStubsProvider {
  private static final Logger LOG = LoggerFactory.getLogger(PssStubsProvider.class);

  private final ManagedChannel channel;
  private final Stubs globalStubs;
  private final ConcurrentMap<StubsKey, Stubs> stubs = new ConcurrentHashMap<>();

  @VisibleForTesting
  PssStubsProvider(final ManagedChannel channel) {
    this.channel = channel;
    this.globalStubs = new Stubs(
        RS3Grpc.newBlockingStub(channel),
        RS3Grpc.newStub(channel)
    );
  }

  static PssStubsProvider connect(
      final String target,
      final boolean useTls
  ) {
    final ChannelCredentials channelCredentials;
    if (useTls) {
      channelCredentials = TlsChannelCredentials.create();
    } else {
      channelCredentials = InsecureChannelCredentials.create();
    }
    LOG.info("Connecting to {}...", target);
    final ManagedChannel channel = Grpc.newChannelBuilder(target, channelCredentials)
        .keepAliveTime(5, TimeUnit.SECONDS)
        .build();
    LOG.info("build grpc client with retries");
    return new PssStubsProvider(channel);
  }

  void close() {
    channel.shutdownNow();
  }

  Stubs stubs(final UUID storeId, final int pssId) {
    final StubsKey key = new StubsKey(storeId, pssId);
    return stubs.computeIfAbsent(key, k -> {
      final RS3Grpc.RS3BlockingStub syncStub = RS3Grpc.newBlockingStub(channel)
          .withInterceptors(new PssHeadersInterceptor(storeId, pssId));
      final RS3Grpc.RS3Stub asyncStub = RS3Grpc.newStub(channel)
          .withInterceptors(new PssHeadersInterceptor(storeId, pssId));
      return new Stubs(syncStub, asyncStub);
    });
  }

  Stubs globalStubs() {
    return globalStubs;
  }

  @VisibleForTesting
  static class Stubs {
    private final RS3Grpc.RS3BlockingStub syncStub;
    private final RS3Grpc.RS3Stub asyncStub;

    @VisibleForTesting
    Stubs(final RS3Grpc.RS3BlockingStub syncStub, final RS3Grpc.RS3Stub asyncStub) {
      this.syncStub = syncStub;
      this.asyncStub = asyncStub;
    }

    RS3Grpc.RS3BlockingStub syncStub() {
      return syncStub;
    }

    RS3Grpc.RS3Stub asyncStub() {
      return asyncStub;
    }
  }

  private static class StubsKey {
    private final UUID storeId;
    private final int pssId;

    private StubsKey(final UUID storeId, final int pssId) {
      this.storeId = storeId;
      this.pssId = pssId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StubsKey)) {
        return false;
      }
      final StubsKey that = (StubsKey) o;
      return pssId == that.pssId && Objects.equals(storeId, that.storeId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(storeId, pssId);
    }
  }
}
