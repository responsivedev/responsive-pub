/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.license;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

class PublicKeyPemFileParserTest {

  @Test
  public void shouldParseValidPemFile() {
    // given:
    final File file = getPemFile("valid.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFile(file);

    // then:
    assertThat(new String(key), is("foobarbaz"));
  }

  @Test
  public void shouldParsePemFileWithRealKey() {
    // given:
    final File file = getPemFile("valid-real.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFile(file);

    // then:
    assertThat(key.length, is(550));
  }

  @Test
  public void shouldParseValidPemFileWithComment() {
    // given:
    final File file = getPemFile("valid-with-comment.pem");

    // when:
    final byte[] key = PublicKeyPemFileParser.parsePemFile(file);

    // then:
    assertThat(new String(key), is("foobarbaz"));
  }

  @Test
  public void shouldFailToParseInvalidPemFileWithMissingFooter() {
    // given:
    final File file = getPemFile("invalid-missing-footer.pem");

    // when/then:
    assertThrows(IllegalArgumentException.class, () -> PublicKeyPemFileParser.parsePemFile(file));
  }

  @Test
  public void shouldFailToParseInvalidPemFileWithMissingHeader() {
    // given:
    final File file = getPemFile("invalid-missing-header.pem");

    // when/then:
    assertThrows(IllegalArgumentException.class, () -> PublicKeyPemFileParser.parsePemFile(file));
  }

  private File getPemFile(final String filename) {
    final String path = "license-test/public-key-pem-file-parser/" + filename;
    try {
      return new File(
          PublicKeyPemFileParserTest.class.getClassLoader().getResource(path).toURI()
      );
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}