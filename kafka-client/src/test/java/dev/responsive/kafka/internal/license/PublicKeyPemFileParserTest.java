/*
 * Copyright 2024 Responsive Computing, Inc.
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
    final String path = "public-key-pem-file-parser-test/" + filename;
    try {
      return new File(
          PublicKeyPemFileParserTest.class.getClassLoader().getResource(path).toURI()
      );
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}