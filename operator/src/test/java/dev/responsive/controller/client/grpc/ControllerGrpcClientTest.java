/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.controller.client.grpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import responsive.controller.v1.controller.proto.ControllerGrpc;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.platform.auth.ApiKeyHeaders;


@ExtendWith(MockitoExtension.class)
class ControllerGrpcClientTest {
  private static final String TARGET = "controller:1234";

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private final ServerInterceptor mockServerInterceptor = mock(ServerInterceptor.class, delegatesTo(
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
          return next.startCall(call, headers);
        }
      }));

  @Mock
  private ManagedChannel channel;

  @Mock
  private ControllerGrpc.ControllerBlockingStub stub;
  @Mock
  private dev.responsive.controller.client.grpc.ControllerGrpcClient.GrpcFactories grpcFactories;

  private dev.responsive.controller.client.grpc.ControllerGrpcClient client;

  private final String apiKey = "apiKey";
  private final String secret = "secret";

  private final ArgumentCaptor<ClientInterceptor> clientInterceptorArgumentCaptor =
      ArgumentCaptor.forClass(ClientInterceptor.class);

  @BeforeEach
  public void setup() {
    when(grpcFactories.createChannel(any(), any(),
        clientInterceptorArgumentCaptor.capture())).thenReturn(channel);
    when(grpcFactories.createBlockingStub(any())).thenReturn(stub);
    lenient().when(stub.withDeadlineAfter(anyLong(), any())).thenReturn(stub);
    client = new dev.responsive.controller.client.grpc.ControllerGrpcClient(
        TARGET,
        apiKey,
        secret,
        false,
        grpcFactories
    );
  }

  @Test
  public void shouldConnectCorrectly() {
    verify(grpcFactories).createChannel(eq(TARGET), any(TlsChannelCredentials.class),
        any(ClientInterceptor.class));
    verify(grpcFactories).createBlockingStub(channel);
  }

  @Test
  public void shouldSendAuthHeadersCorrectly() throws Exception {
    // In this test we use the captured client header interceptor and ensure that the headers go out
    // to the server correctly. We use a fake in process server and then verify the metadata on the
    // server side.

    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(InProcessServerBuilder.forName(serverName).directExecutor()
        .addService(ServerInterceptors.intercept(new SimpleServiceGrpc.SimpleServiceImplBase() {
        }, mockServerInterceptor))
        .build().start());
    // Create a client channel and register for automatic graceful shutdown.
    ManagedChannel channel = grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build());

    SimpleServiceGrpc.SimpleServiceBlockingStub blockingStub = SimpleServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, clientInterceptorArgumentCaptor.getValue()));

    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);

    try {
      blockingStub.unaryRpc(SimpleRequest.getDefaultInstance());
      fail();
    } catch (StatusRuntimeException expected) {
      // expected because the method is not implemented at server side
    }

    verify(mockServerInterceptor).interceptCall(
        ArgumentMatchers.<ServerCall<SimpleRequest, SimpleResponse>>any(),
        metadataCaptor.capture(),
        ArgumentMatchers.<ServerCallHandler<SimpleRequest, SimpleResponse>>any());

    final String receivedApiKey = metadataCaptor.getValue().get(Metadata.Key.of(
        ApiKeyHeaders.API_KEY_METADATA_KEY,
        Metadata.ASCII_STRING_MARSHALLER));
    assertEquals(apiKey, receivedApiKey);

    final String receivedSecret = metadataCaptor.getValue().get(Metadata.Key.of(
        ApiKeyHeaders.SECRET_METADATA_KEY,
        Metadata.ASCII_STRING_MARSHALLER));
    assertEquals(secret, receivedSecret);
  }


  @Test
  public void shouldSendUpsertPolicyRequest() {
    // given:
    final var req = ControllerOuterClass.UpsertPolicyRequest.newBuilder().build();
    when(stub.upsertPolicy(any())).thenReturn(
        ControllerOuterClass.SimpleResponse.newBuilder().build());

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
    when(stub.currentState(any())).thenReturn(
        ControllerOuterClass.SimpleResponse.newBuilder().build());

    // when:
    client.currentState(req);

    // then:
    verify(stub).currentState(req);
  }

  @Test
  public void shouldHandleCurrentStatusRequestError() {
    // given:
    final var req = ControllerOuterClass.CurrentStateRequest.newBuilder().build();
    when(stub.currentState(any())).thenReturn(ControllerOuterClass.SimpleResponse.newBuilder()
        .setError("oops")
        .build()
    );

    // when/then:
    assertThrows(RuntimeException.class, () -> client.currentState(req));
  }

  @Test
  public void shouldSendTargetStatusRequest() {
    // given:
    final var req = ControllerOuterClass.EmptyRequest.newBuilder().build();
    final var state = ControllerOuterClass.ApplicationState.newBuilder().build();
    when(stub.getTargetState(any())).thenReturn(
        ControllerOuterClass.GetTargetStateResponse.newBuilder()
            .setState(state)
            .build()
    );

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
    when(stub.getTargetState(any())).thenReturn(
        ControllerOuterClass.GetTargetStateResponse.newBuilder()
            .setError("oops")
            .build()
    );

    // when:
    assertThrows(RuntimeException.class, () -> client.getTargetState(req));
  }
}