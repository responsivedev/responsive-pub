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
package dev.responsive.kafka.store;

import static dev.responsive.utils.StoreUtil.validateLogConfigs;

import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class ResponsiveMaterialized<K, V, S extends StateStore> extends Materialized<K, V, S> {

  private final boolean truncateChangelog;

  public ResponsiveMaterialized(
      final Materialized<K, V, S> materialized,
      final boolean truncateChangelog
  ) {
    super(materialized);
    this.truncateChangelog = truncateChangelog;
    if (truncateChangelog) {
      topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    }
  }

  @Override
  public Materialized<K, V, S> withLoggingEnabled(final Map<String, String> config) {
    validateLogConfigs(config, truncateChangelog);
    return super.withLoggingEnabled(config);
  }
}
