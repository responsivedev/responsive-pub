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

package dev.responsive.controller.client;

import java.util.List;
import responsive.controller.v1.controller.proto.ControllerOuterClass.Action;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;
import responsive.controller.v1.controller.proto.ControllerOuterClass.CurrentStateRequest;
import responsive.controller.v1.controller.proto.ControllerOuterClass.EmptyRequest;
import responsive.controller.v1.controller.proto.ControllerOuterClass.UpdateActionStatusRequest;
import responsive.controller.v1.controller.proto.ControllerOuterClass.UpsertPolicyRequest;

/**
 * Client for sending requests to the controller. The client
 * must initiate all requests with the controller in order to
 * satisfy standard network configurations.
 */
public interface ControllerClient {

  /**
   * Insert or replace an existing Policy
   *
   * @param request the upsert policy request
   */
  void upsertPolicy(final UpsertPolicyRequest request);

  void currentState(final CurrentStateRequest request);

  /**
   * @param request an empty request containing only the application id
   * @return the target application state that the operator should resolve
   */
  ApplicationState getTargetState(final EmptyRequest request);

  List<Action> getCurrentActions(final EmptyRequest request);

  void updateActionStatus(final UpdateActionStatusRequest request);
}
