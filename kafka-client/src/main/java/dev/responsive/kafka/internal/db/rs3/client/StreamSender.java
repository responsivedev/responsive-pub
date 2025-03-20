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

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.concurrent.CompletionStage;

public interface StreamSender<S> {
  /**
   * Send the next message to the stream. If an exception is raised
   * in this call, then the sender will no longer be usable and attempting
   * to send another message will result in a
   *
   * @param msg the message to send to the stream
   * @throws RS3Exception in the case of an unexpected error from RS3
   * @throws IllegalStateException if invoked following a raised exception or
   *   a call to either {@link #finish()}, or {@link #cancel()}.
   */
  void sendNext(S msg);

  /**
   * This method marks the end of the stream indicating that there are no
   * further messages to send.
   *
   * @throws RS3Exception in the case of an unexpected error from RS3
   * @throws IllegalStateException if invoked following a raised exception in
   *   {@link #sendNext(Object)} or a call to {@link #cancel()}.
   */
  void finish();

  /**
   * Cancel the stream. This is used to signal the server about unexpected
   * client errors so that it can clean up resources.
   *
   * @throws RS3Exception in the case of an unexpected error from RS3
   * @throws IllegalStateException if invoked following a raised exception in
   *   {@link #sendNext(Object)} or a call to {@link #finish()}.
   */
  void cancel();

  /**
   * Get a completion stage tied to this stream sender. The stage is completed
   * upon an explicit call to {@link #finish()}, {@link #cancel()}, or if an error
   * is raised while sending or finishing.
   *
   * @return a completion stage for this sender
   */
  CompletionStage<Void> completion();

  /**
   * Check whether the stream is complete (either finished or failed).
   *
   * @return true if the stream has finished or failed
   */
  boolean isDone();
}
