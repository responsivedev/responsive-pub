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

package dev.responsive.kafka.internal.license;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PublicKeyPemFileParserTest {

  @Test
  public void shouldParseValidPemFile() {
    // given:
    final String file = getPemFile("valid.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFileInResource(file);

    // then:
    assertThat(new String(key), is("foobarbaz"));
  }

  @Test
  public void shouldParsePemFileWithRealKey() {
    // given:
    final String file = getPemFile("valid-real.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFileInResource(file);

    // then:
    assertThat(key.length, is(550));
  }

  @Test
  public void shouldParseValidPemFileWithComment() {
    // given:
    final String file = getPemFile("valid-with-comment.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFileInResource(file);

    // then:
    assertThat(new String(key), is("foobarbaz"));
  }

  @Test
  public void shouldFailToParseInvalidPemFileWithMissingFooter() {
    // given:
    final String file = getPemFile("invalid-missing-footer.pem");

    // when/then:
    assertThrows(
        IllegalArgumentException.class, () -> PublicKeyPemFileParser.parsePemFileInResource(file));
  }

  @Test
  public void shouldFailToParseInvalidPemFileWithMissingHeader() {
    // given:
    final String file = getPemFile("invalid-missing-header.pem");

    // when/then:
    assertThrows(
        IllegalArgumentException.class, () -> PublicKeyPemFileParser.parsePemFileInResource(file));
  }

  private String getPemFile(final String filename) {
    return "/public-key-pem-file-parser-test/" + filename;
  }
}