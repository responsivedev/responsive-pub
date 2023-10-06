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
import java.util.LinkedHashMap;

@SuppressWarnings("checkstyle:linelength")
public class TopicMetrics implements MetricGroup {
  
  public static final String TOPIC_METRIC_GROUP = "topic-metrics";

  public static final String COMMITTED_OFFSET = "committed-offset";
  public static final String COMMITTED_OFFSET_DESCRIPTION = "The latest committed offset of this topic partition";

  public static final String END_OFFSET = "end-offset";
  public static final String END_OFFSET_DESCRIPTION = "The end offset of this topic partition";

  private final LinkedHashMap<String, String> tags;

  TopicMetrics(final LinkedHashMap<String, String> tags) {
    this.tags = tags;
  }
  
  @Override
  public String groupName() {
    return TOPIC_METRIC_GROUP;
  }

  @Override
  public LinkedHashMap<String, String> tags() {
    return tags;
  }
}
