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

package dev.responsive.kafka.internal.db.partitioning;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.db.partitioning.Murmur3Hasher;
import org.junit.jupiter.api.Test;

class Murmur3HasherTest {

  @Test
  public void shouldNotChangeSalt() {
    assertThat(
        "changing the salt from 31 to another number is backwards incompatible!",
        Murmur3Hasher.SALT,
        is(31)
    );
  }

}