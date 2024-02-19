package dev.responsive.kafka.internal.stores;

import com.google.common.collect.ImmutableList;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueBytesStoreSupplier;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreSupplier;

public class StoreAccumulator {
  public static final StoreAccumulator INSTANCE = new StoreAccumulator();

  private final List<ResponsiveKeyValueBytesStoreSupplier> registeredKeyValueBytesStoreSuppliers
      = new LinkedList<>();

  private StoreAccumulator() {
  }

  public void register(final ResponsiveKeyValueBytesStoreSupplier instance) {
    registeredKeyValueBytesStoreSuppliers.add(instance);
  }

  public List<ResponsiveKeyValueBytesStoreSupplier> getRegisteredKeyValueBytesStoreSuppliers() {
    return ImmutableList.copyOf(registeredKeyValueBytesStoreSuppliers);
  }
}
