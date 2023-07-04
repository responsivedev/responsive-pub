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

package dev.responsive.utils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

public class TestUtils {

  private TestUtils() {

  }

  public static void awaitTopics(final Admin admin, final List<NewTopic> topics)
      throws TimeoutException, InterruptedException {
    final long start = System.nanoTime();
    while (true) {
      try {
        admin.createTopics(topics).all().get();
        break;
      } catch (ExecutionException | InterruptedException e) {
        if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(10)) {
          throw new TimeoutException("Failed to create topics via admin call.");
        }
        Thread.sleep(100);
      }
    }
  }

}
