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

package dev.responsive.kafka.internal.metrics;

import dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MetricGroup;
import java.util.HashMap;
import java.util.Map;

public class ApplicationMetrics implements MetricGroup {

  // Responsive application metrics scoped to the Streams client level
  public static final String APPLICATION_METRIC_GROUP = "application.metrics";

  public static final String STREAMS_STATE = "streams-state";
  public static final String STREAMS_STATE_DESCRIPTION = "The current KafkaStreams.State expressed as its ordinal value";

  public static final String NUM_RESTORING = "num-restoring-changelogs";
  public static final String NUM_RESTORING_DESCRIPTION = "The number of changelog topics currently being restored from";

  public static final String NUM_INTERRUPTED = "num-interrupted-changelogs";
  public static final String NUM_INTERRUPTED_DESCRIPTION = "The total number of changelog partitions that began restoring but did not complete";

  private final Map<String, String> tags = new HashMap<>();
  
  ApplicationMetrics(final Map<String, String> baseAppTags) {
    tags.putAll(baseAppTags);
  }

  @Override
  public String groupName() {
    return APPLICATION_METRIC_GROUP;
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }
}
