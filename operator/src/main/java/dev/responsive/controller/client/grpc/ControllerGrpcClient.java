/*
 * Copyright 2023 Responsive Computing, Inc.
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

package dev.responsive.controller.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.controller.client.ControllerClient;
import io.grpc.ChannelCredentials;
import io.grpc.ClientInterceptor;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.TimeUnit;
import responsive.controller.v1.controller.proto.ControllerGrpc;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.platform.auth.ApiKeyHeaders;

public class ControllerGrpcClient implements ControllerClient {
  private final ManagedChannel channel;
  private final ControllerGrpc.ControllerBlockingStub stub;

  public ControllerGrpcClient(final String target, final String apiKey, final String secret) {
    this(target, apiKey, secret, new GrpcFactories() {});
  }

  @VisibleForTesting
  ControllerGrpcClient(final String target, final String apiKey, final String secret,
                       final GrpcFactories grpcFactories) {

    final Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of(
        ApiKeyHeaders.API_KEY_METADATA_KEY, Metadata.ASCII_STRING_MARSHALLER), apiKey);
    metadata.put(Metadata.Key.of(
        ApiKeyHeaders.SECRET_METADATA_KEY, Metadata.ASCII_STRING_MARSHALLER), secret);

    channel = grpcFactories.createChannel(target, InsecureChannelCredentials.create(),
            MetadataUtils.newAttachHeadersInterceptor(metadata));
    stub = grpcFactories.createBlockingStub(channel);
  }

  @Override
  public void upsertPolicy(final ControllerOuterClass.UpsertPolicyRequest request) {
    final var rsp = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
        .upsertPolicy(request);
    throwOnError(rsp);
  }

  @Override
  public void currentState(final ControllerOuterClass.CurrentStateRequest request) {
    final var rsp = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
        .currentState(request);
    throwOnError(rsp);
  }

  @Override
  public ControllerOuterClass.ApplicationState getTargetState(
      final ControllerOuterClass.EmptyRequest request) {
    final var rsp = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
        .getTargetState(request);
    if (!rsp.getError().equals("")) {
      throw new RuntimeException(rsp.getError());
    }
    return rsp.getState();
  }

  private void throwOnError(final ControllerOuterClass.SimpleResponse rsp) {
    if (!rsp.getError().equals("")) {
      // TODO(rohan): use a better error class
      throw new RuntimeException(rsp.getError());
    }
  }

  interface GrpcFactories {
    default ManagedChannel createChannel(final String target, final ChannelCredentials credentials,
                                         ClientInterceptor addHeadersInterceptor) {

      return Grpc.newChannelBuilder(target, credentials)
              .intercept(addHeadersInterceptor)
              .build();
    }

    default ControllerGrpc.ControllerBlockingStub createBlockingStub(final ManagedChannel channel) {
      return ControllerGrpc.newBlockingStub(channel);
    }
  }
}
