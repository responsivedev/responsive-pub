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

package dev.responsive.kafka.testutils;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

public class MultiStateStoreContext implements InvocationHandler, StateRestoreCallback,
    CommitCallback {
  private final StateStoreContext delegate;
  private final StateStore root;

  private final List<StateRestoreCallback> restoreCallbacks;
  private final List<CommitCallback> commitCallbacks;

  private boolean registered;

  public MultiStateStoreContext(StateStoreContext delegate, StateStore root) {
    this.delegate = delegate;
    this.root = root;

    this.registered = false;
    this.restoreCallbacks = new ArrayList<>();
    this.commitCallbacks = new ArrayList<>();
  }

  @Override
  public Object invoke(final Object proxy, final Method method, final Object[] args)
      throws Throwable {
    if (method.getName().equals("register")) {
      if (args.length == 2) {
        this.register(args[0], args[1]);
        return null;
      } else if (args.length == 3) {
        this.register(args[0], args[1], args[2]);
        return null;
      } else {
        throw new RuntimeException("Failed to proxy register call.");
      }
    }
    return method.invoke(this.delegate, args);
  }

  public void register(
      final Object stateStore,
      final Object stateRestoreCallback
  ) {
    this.register((StateStore) stateStore, (StateRestoreCallback) stateRestoreCallback);
  }

  public void register(
      final Object stateStore,
      final Object stateRestoreCallback,
      final Object commitCallback
  ) {
    this.register((StateStore) stateStore, (StateRestoreCallback) stateRestoreCallback,
        (CommitCallback) commitCallback);
  }

  public void register(
      final StateStore stateStore,
      final StateRestoreCallback stateRestoreCallback
  ) {
    this.restoreCallbacks.add(stateRestoreCallback);

    if (!this.registered) {
      this.delegate.register(this.root, this, this);
      this.registered = true;
    }
  }

  public void register(
      final StateStore stateStore,
      final StateRestoreCallback stateRestoreCallback,
      final CommitCallback commitCallback
  ) {
    this.restoreCallbacks.add(stateRestoreCallback);
    this.commitCallbacks.add(commitCallback);

    if (!this.registered) {
      this.delegate.register(this.root, this, this);
      this.registered = true;
    }
  }

  @Override
  public void restore(final byte[] bytes, final byte[] bytes1) {
    this.restoreCallbacks.forEach(callback -> callback.restore(bytes, bytes1));
  }

  @Override
  public void onCommit() throws IOException {
    for (var callback : this.commitCallbacks) {
      callback.onCommit();
    }
  }
}
