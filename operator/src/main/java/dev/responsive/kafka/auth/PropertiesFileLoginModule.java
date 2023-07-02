package dev.responsive.kafka.auth;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertiesFileLoginModule implements LoginModule {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesFileLoginModule.class);

  private static final String PROPERTIES_FILE = "properties";
  private static final String USERNAME_CONFIG = "username";
  private static final String PASSWORD_CONFIG = "password";

  @Override
  public void initialize(
      final Subject subject,
      final CallbackHandler callbackHandler,
      final Map<String, ?> sharedState,
      final Map<String, ?> options
  ) {
    final String propertiesPath = (String) options.get(PROPERTIES_FILE);
    if (propertiesPath == null) {
      LOG.info("no properties file specified in JAAS config");
      return;
    }
    final Properties properties = new Properties();
    try (InputStream in = new FileInputStream(propertiesPath)) {
      properties.load(in);
    } catch (final IOException e) {
      LOG.info("error loading properties file", e);
    }
    String username = properties.getProperty(USERNAME_CONFIG);
    if (username != null) {
      LOG.info("login using name " + username);
      subject.getPublicCredentials().add(username);
    }
    String password = properties.getProperty(PASSWORD_CONFIG);
    if (password != null) {
      subject.getPrivateCredentials().add(password);
    }
  }

  @Override
  public boolean login() {
    return true;
  }

  @Override
  public boolean logout() {
    return true;
  }

  @Override
  public boolean commit() {
    return true;
  }

  @Override
  public boolean abort() {
    return false;
  }
}
