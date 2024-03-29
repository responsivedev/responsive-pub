package dev.responsive.kafka.api.async.internals.contexts;

public class AsyncProcessorContext<KOut, VOut>
    extends DelegatingInternalProcessorContext<KOut, VOut, MergedProcessorContext<KOut, VOut>> {
  private final ThreadLocal<MergedProcessorContext<KOut, VOut>> tlDelegate = new ThreadLocal<>();

  @Override
  public MergedProcessorContext<KOut, VOut> delegate() {
    // todo: assert this is only called from stream thread or async processor thread
    return tlDelegate.get();
  }

  public void setDelegateForCurrentThread(final MergedProcessorContext<KOut, VOut> delegate) {
    tlDelegate.set(delegate);
  }
}