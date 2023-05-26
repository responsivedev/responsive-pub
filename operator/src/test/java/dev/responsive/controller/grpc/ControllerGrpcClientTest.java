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

package dev.responsive.controller.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import responsive.controller.v1.controller.proto.ControllerGrpc;
import responsive.controller.v1.controller.proto.ControllerOuterClass;


@ExtendWith(MockitoExtension.class)
class ControllerGrpcClientTest {
  private static final String TARGET = "controller:1234";

  @Mock
  private ManagedChannel channel;
  @Mock
  private ControllerGrpc.ControllerBlockingStub stub;
  @Mock
  private ControllerGrpcClient.GrpcFactories grpcFactories;

  private ControllerGrpcClient client;

  @BeforeEach
  public void setup() {
    when(grpcFactories.createChannel(any(), any())).thenReturn(channel);
    when(grpcFactories.createBlockingStub(any())).thenReturn(stub);
    lenient().when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
    client = new ControllerGrpcClient(TARGET, grpcFactories);
  }

  @Test
  public void shouldConnectCorrectly() {
    verify(grpcFactories).createChannel(eq(TARGET), any(InsecureChannelCredentials.class));
    verify(grpcFactories).createBlockingStub(channel);
  }

  @Test
  public void shouldSendUpsertPolicyRequest() {
    // given:
    final var req = ControllerOuterClass.UpsertPolicyRequest.newBuilder().build();
    when(stub.upsertPolicy(any()))
        .thenReturn(ControllerOuterClass.SimpleResponse.newBuilder().build());

    // when:
    client.upsertPolicy(req);

    // then:
    verify(stub).upsertPolicy(req);
  }

  @Test
  public void shouldHandleUpsertPolicyRequestError() {
    // given:
    final var req = ControllerOuterClass.UpsertPolicyRequest.newBuilder().build();
    when(stub.upsertPolicy(any())).thenReturn(ControllerOuterClass.SimpleResponse.newBuilder()
        .setError("oops")
        .build()
    );

    // when/then:
    assertThrows(RuntimeException.class, () -> client.upsertPolicy(req));
  }

  @Test
  public void shouldSendCurrentStatusRequest() {
    // given:
    final var req = ControllerOuterClass.CurrentStateRequest.newBuilder().build();
    when(stub.currentState(any()))
        .thenReturn(ControllerOuterClass.SimpleResponse.newBuilder().build());

    // when:
    client.currentState(req);

    // then:
    verify(stub).currentState(req);
  }

  @Test
  public void shouldHandleCurrentStatusRequestError() {
    // given:
    final var req = ControllerOuterClass.CurrentStateRequest.newBuilder().build();
    when(stub.currentState(any()))
        .thenReturn(ControllerOuterClass.SimpleResponse.newBuilder()
        .setError("oops")
        .build());

    // when/then:
    assertThrows(RuntimeException.class, () -> client.currentState(req));
  }

  @Test
  public void shouldSendTargetStatusRequest() {
    // given:
    final var req = ControllerOuterClass.EmptyRequest.newBuilder().build();
    final var state = ControllerOuterClass.ApplicationState.newBuilder().build();
    when(stub.getTargetState(any()))
        .thenReturn(ControllerOuterClass.GetTargetStateResponse.newBuilder()
        .setState(state)
        .build());

    // when:
    final var returnedState = client.getTargetState(req);

    // then:
    assertThat(returnedState, is(state));
    verify(stub).getTargetState(req);
  }

  @Test
  public void shouldHandleTargetStatusRequestError() {
    // given:
    final var req = ControllerOuterClass.EmptyRequest.newBuilder().build();
    when(stub.getTargetState(any()))
        .thenReturn(ControllerOuterClass.GetTargetStateResponse.newBuilder()
        .setError("oops")
        .build());

    // when:
    assertThrows(RuntimeException.class, () -> client.getTargetState(req));
  }
}