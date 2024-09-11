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

package dev.responsive.kafka.internal.db.spec;

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy;
import dev.responsive.kafka.internal.db.TableOperations;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class TimeWindowedCompactionTableSpec<K, V> extends DelegatingTableSpec<K, V> {

  public TimeWindowedCompactionTableSpec(final RemoteTableSpec<K, V> delegate) {
    super(delegate);
  }

  @Override
  public EnumSet<TableOperations> restrictedOperations() {
    // TODO(agavra): we should restrict PUT, PUT_ALL and DELETE in this spec
    return super.restrictedOperations();
  }

  @Override
  public CreateTableWithOptions applyOptions(final CreateTableWithOptions base) {
    if (!(delegate() instanceof TtlTableSpec)) {
      // TODO(agavra): consider whether we want to support TWCS if there's no TTL
      return base.withCompaction(SchemaBuilder.timeWindowCompactionStrategy());
    }

    final var delegate = (TtlTableSpec) delegate();
    final var compactionWindow = Duration.ofSeconds(delegate.ttlSeconds() / 20);
    final var strategy =
        SchemaBuilder.timeWindowCompactionStrategy()
            .withCompactionWindow(
                compactionWindow.toMinutes(),
                TimeWindowCompactionStrategy.CompactionWindowUnit.MINUTES);

    return delegate.applyOptions(base)
        .withCompaction(strategy)
        // without setting a low gc_grace_seconds, we will have to wait an
        // additional 10 days (the default gc_grace_seconds) before any SST
        // on disk is removed. setting a good value for this is somewhat a
        // "dark art"
        //
        // there's no reason to hold on to tombstones (or hints) for more than
        // the TTL since any data that was written more than ttlSeconds ago
        // does not need to be replicated to a node that has been down for the
        // entirety of ttlSeconds
        //
        // we choose 6 hours for the minimum value since that's 2x the default
        // retention for hints (hinted handoff). handoffs are disabled by default
        // in cloud, so we could theoretically set this to 0, but holding on to
        // an extra 6 hours of data when TTL is larger than 6 hours anyway isn't
        // too big of a concern (and it can always be tweaked afterwards)
        //
        // see this blog for mor information:
        // https://thelastpickle.com/blog/2018/03/21/hinted-handoff-gc-grace-demystified.html
        .withGcGraceSeconds(Math.min(delegate.ttlSeconds(), (int) TimeUnit.HOURS.toSeconds(6)));
  }
}
