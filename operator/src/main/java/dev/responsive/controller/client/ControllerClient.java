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
