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
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoTimeoutException;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
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
    if (shouldLogError(exception, new LinkedList<>())) {
      LOG.error("uncaught exception on test app stream thread {}({}) {}",
          exception.getClass().getName(),
          exception.getMessage(),
          causalSummary(exception, new LinkedList<>()),
          exception
      );
      Assert.unreachable("Uncaught exception on test app stream thread", null);
    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  private String causalSummary(final Throwable t, final List<Throwable> seen) {
    final String summary = t.getClass().getName() + "->";
    seen.add(t);
    if (t.getCause() == null || seen.contains(t.getCause())) {
      return summary + causalSummary(t.getCause(), seen);
    }
    return summary;
  }

  private boolean shouldLogError(final Throwable throwable, List<Throwable> seen) {
    final List<Class<? extends Throwable>> dontcare = List.of(
        AllNodesFailedException.class,
        ConnectException.class,
        ConnectionInitException.class,
        DisconnectException.class,
        DriverTimeoutException.class,
        InjectedE2ETestException.class,
        InvalidProducerEpochException.class,
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
        return false;
      }
    }

    if (throwable instanceof MongoQueryException && throwable.getMessage().contains(
            "Command failed with error 13436 (NotPrimaryOrSecondary)")
    ) {
      return false;
    }

    seen.add(throwable);
    if (throwable.getCause() != null && !seen.contains(throwable.getCause())) {
      return shouldLogError(throwable.getCause(), seen);
    }
    return true;
  }
}
