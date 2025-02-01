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

package dev.responsive.examples.common;

import com.antithesis.sdk.Assert;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UncaughtStreamsAntithesisHandler implements StreamsUncaughtExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UncaughtStreamsAntithesisHandler.class);

  @Override
  public StreamThreadExceptionResponse handle(final Throwable exception) {
    final Optional<Throwable> realErrorCause = rootCauseForRealError(exception, new LinkedList<>());
    if (realErrorCause.isPresent()) {
      LOG.error(
          String.format("uncaught exception on test app stream thread %s(%s), caused by %s(%s): %s",
                        exception.getClass().getName(),
                        exception.getMessage(),
                        realErrorCause.get().getClass().getName(),
                        realErrorCause.get().getMessage(),
                        causalSummary(exception, new LinkedList<>())
          ), exception);

      final ObjectNode assertNode = new ObjectMapper().createObjectNode();
      assertNode.put("exceptionClass", exception.getClass().getName());
      assertNode.put("exceptionMessage", exception.getMessage());

      // may or may not be different than the "root cause", sometimes both are useful
      if (exception.getCause() != null) {
        assertNode.put("causeClass", exception.getCause().getClass().getName());
        assertNode.put("causeMessage", exception.getCause().getMessage());
      }

      assertNode.put("rootCauseClass", realErrorCause.get().getClass().getName());
      assertNode.put("rootCauseMessage", realErrorCause.get().getMessage());

      assertNode.put("summary", causalSummary(exception, new LinkedList<>()));
      Assert.unreachable("Uncaught exception on test app stream thread", assertNode);

    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  private String causalSummary(final Throwable t, final List<Throwable> seen) {
    final String summary = t.getClass().getName() + "->";
    seen.add(t);
    if (t.getCause() != null && !seen.contains(t.getCause())) {
      return summary + causalSummary(t.getCause(), seen);
    }
    return summary;
  }

  /**
   * @return the root cause error if this is a "real" error that should be logged,
   *         and Optional.empty if this is an expected exception type
   */
  private Optional<Throwable> rootCauseForRealError(
      final Throwable throwable,
      List<Throwable> seen
  ) {
    if (throwable instanceof InjectedE2ETestException) {
      final ObjectNode assertNode = new ObjectMapper().createObjectNode();
      assertNode.put("seenExceptions", seen.toString());
      Assert.reachable("Caught injected e2e test exception", assertNode);
      return Optional.empty();
    }

    final List<Class<? extends Throwable>> dontcare = List.of(
        AllNodesFailedException.class,
        ConnectException.class,
        ConnectionInitException.class,
        DisconnectException.class,
        DriverTimeoutException.class,
        InjectedE2ETestException.class,
        InvalidProducerEpochException.class,
        MongoNodeIsRecoveringException.class,
        MongoNotPrimaryException.class,
        MongoSocketReadException.class,
        MongoTimeoutException.class,
        ProducerFencedException.class,
        ReadFailureException.class,
        ReadTimeoutException.class,
        TimeoutException.class,
        java.util.concurrent.TimeoutException.class,
        TransactionAbortedException.class,
        UnavailableException.class,
        WriteFailureException.class,
        WriteTimeoutException.class
    );
    for (final var c : dontcare) {
      if (c.isInstance(throwable)) {
        return Optional.empty();
      }
    }

    if (throwable instanceof MongoQueryException && throwable.getMessage().contains(
            "Command failed with error 13436 (NotPrimaryOrSecondary)")
    ) {
      return Optional.empty();
    }

    seen.add(throwable);
    if (throwable.getCause() != null && !seen.contains(throwable.getCause())) {
      return rootCauseForRealError(throwable.getCause(), seen);
    }

    return Optional.of(throwable);
  }
}
