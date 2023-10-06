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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;

public class OrderedTagsSupplierTest {

  private static final String RESPONSIVE_VERSION = "0.666.0";
  private static final String RESPONSIVE_COMMIT = "abcde";
  private static final String STREAMS_VERSION = "1.2.3";
  private static final String STREAMS_COMMIT = "zyxwv";

  private static final String APPLICATION_ID = "ordered-tags-test";
  private static final String CLIENT_ID = "ordered-tags-test-node-1";

  private static final Map<String, ?> userTags = Map.of(
      "custom-tag-1", "C",
      "custom-tag-2", "B",
      "custom-tag-3", "A"
  );

  private final OrderedTagsSupplier tagSupplier = new OrderedTagsSupplier(
      RESPONSIVE_VERSION,
      RESPONSIVE_COMMIT,
      STREAMS_VERSION,
      STREAMS_COMMIT,
      APPLICATION_ID,
      APPLICATION_ID,
      CLIENT_ID,
      userTags
  );

  @Test
  public void shouldReturnApplicationGroupTagsInOrder() {
    final Map<String, String> tags = tagSupplier.applicationGroupTags();
    final Iterator<Entry<String, String>> tagsIter = tags.entrySet().iterator();

    Map.Entry<String, String> tag = tagsIter.next();
    assertThat(tag, equalTo());

  }

}
