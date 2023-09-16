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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

public class ResponsiveStoreBuilder<T extends StateStore> implements StoreBuilder<T> {

  private final StoreBuilder<T> delegate;
  private final boolean truncateChangelog;

  public ResponsiveStoreBuilder(
      final StoreBuilder<T> delegate,
      final boolean truncateChangelog
  ) {
    this.delegate = delegate;
    this.truncateChangelog = truncateChangelog;
  }

  @Override
  public StoreBuilder<T> withCachingEnabled() {
    delegate.withCachingEnabled();
    return this;
  }

  @Override
  public StoreBuilder<T> withCachingDisabled() {
    delegate.withCachingDisabled();
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
    validateLogConfigs(config, truncateChangelog, name());

    delegate.withLoggingEnabled(config);
    return this;
  }

  @Override
  public StoreBuilder<T> withLoggingDisabled() {
    delegate.withLoggingDisabled();
    throw new UnsupportedOperationException(
        "Responsive stores are currently incompatible with disabling the changelog. "
            + "Please reach out to us to request this feature.");
  }

  @Override
  public T build() {
    return delegate.build();
  }

  @Override
  public Map<String, String> logConfig() {
    return delegate.logConfig();
  }

  @Override
  public boolean loggingEnabled() {
    return delegate.loggingEnabled();
  }

  @Override
  public String name() {
    return delegate.name();
  }
}
