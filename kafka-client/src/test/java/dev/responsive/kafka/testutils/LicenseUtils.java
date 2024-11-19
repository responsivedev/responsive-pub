package dev.responsive.kafka.testutils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Base64;

public final class LicenseUtils {
  private static final String DECODED_LICENSE_FILE
      = "test-licenses/test-license.json";

  private LicenseUtils () {
  }

  public static String getLicense() {
    return getEncodedLicense(DECODED_LICENSE_FILE);
  }

  public static String getEncodedLicense(final String filename) {
    return Base64.getEncoder().encodeToString(slurpFile(filename));
  }

  private static byte[] slurpFile(final String filename) {
    try {
      final File file = new File(LicenseUtils.class.getClassLoader()
          .getResource(filename)
          .toURI()
      );
      return Files.readAllBytes(file.toPath());
    } catch (final IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }


}
