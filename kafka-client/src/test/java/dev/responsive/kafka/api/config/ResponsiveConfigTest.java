package dev.responsive.kafka.api.config;

import static dev.responsive.kafka.api.config.ResponsiveConfigTest.MyRS3ConfigSetter.BASIC_KV_FILTER_BITS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.responsive.kafka.api.config.RS3StoreParams.RS3KVStoreParams;
import dev.responsive.kafka.api.config.RS3StoreParams.RS3SessionStoreParams;
import dev.responsive.kafka.api.config.RS3StoreParams.RS3WindowStoreParams;
import dev.responsive.kafka.internal.stores.SchemaTypes.KVSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.SessionSchema;
import dev.responsive.kafka.internal.stores.SchemaTypes.WindowSchema;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ResponsiveConfigTest {

  @Test
  public void testRs3RetryTimeoutConfig() {
    var props = new Properties();
    var config = ResponsiveConfig.responsiveConfig(props);
    assertEquals(
        ResponsiveConfig.RS3_RETRY_TIMEOUT_DEFAULT,
        config.getLong(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG)
    );

    props.setProperty(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG, "10");
    var reconfig = ResponsiveConfig.responsiveConfig(props);
    assertEquals(
        10L,
        reconfig.getLong(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG)
    );
  }

  @Test
  public void shouldGetConfiguredRS3ConfigSetter() {
    final int customBasicKVFilterBits = 5;
    final var props = new HashMap<>();

    props.put(
        ResponsiveConfig.RS3_CONFIG_SETTER_CLASS_CONFIG,
        MyRS3ConfigSetter.class.getName()
    );
    props.put(BASIC_KV_FILTER_BITS_CONFIG, customBasicKVFilterBits);
    final var config = ResponsiveConfig.responsiveConfig(props);
    final RS3ConfigSetter configSetter = config.getRS3ConfigSetter();

    // for basic key value stores use the configured custom filterBits
    assertEquals(
        Optional.of(customBasicKVFilterBits),
        configSetter.keyValueStoreConfig("kv", KVSchema.KEY_VALUE).filterBitsPerKey()
    );

    // for fact stores we're using the fact table default which is 20
    assertEquals(
        Optional.of(20),
        configSetter.keyValueStoreConfig("fact", KVSchema.FACT).filterBitsPerKey()
    );

    // for everything else use the default which is Optional.empty (ie true backend default)
    assertEquals(
        Optional.empty(),
        configSetter.windowStoreConfig("window", WindowSchema.WINDOW).filterBitsPerKey()
    );
    assertEquals(
        Optional.empty(),
        configSetter.sessionStoreConfig("session", SessionSchema.SESSION).filterBitsPerKey()
    );

  }

  public static class MyRS3ConfigSetter implements RS3ConfigSetter {

    public static final String BASIC_KV_FILTER_BITS_CONFIG = "basic.kv.filter.bits";
    private int defaultFilterBits;

    @Override
    public void configure(final Map<String, ?> configs) {
      defaultFilterBits = (int) configs.get(BASIC_KV_FILTER_BITS_CONFIG);
    }

    @Override
    public RS3KVStoreParams keyValueStoreConfig(final String storeName, final KVSchema schema) {
      if (schema == KVSchema.FACT) {
        return RS3KVStoreParams.defaults(schema);
      } else {
        return RS3KVStoreParams.defaults(schema).withFilterBitsPerKey(defaultFilterBits);
      }
    }

    @Override
    public RS3WindowStoreParams windowStoreConfig(
        final String storeName,
        final WindowSchema schema
    ) {
      return RS3WindowStoreParams.defaults(schema);
    }

    @Override
    public RS3SessionStoreParams sessionStoreConfig(
        final String storeName,
        final SessionSchema schema
    ) {
      return RS3SessionStoreParams.defaults(schema);
    }
  }

}