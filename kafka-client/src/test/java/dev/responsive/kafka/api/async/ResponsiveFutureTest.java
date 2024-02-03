package dev.responsive.kafka.api.async;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveFutureTest {
  @Spy
  private CompletableFuture<String> nested1;
  @Spy
  private CompletableFuture<String> nested2;
  @Spy
  private CompletableFuture<String> inner;

  private ResponsiveFuture<String> responsiveFuture;

  @BeforeEach
  public void setup() {
    responsiveFuture = ResponsiveFuture.of(inner);
  }
  @Test
  public void shouldHandleMultipleNestedApplies() throws InterruptedException, ExecutionException {
    // given:
    inner.complete("inner done");
    final ResponsiveFuture<String> applied = responsiveFuture
        .thenApply(s -> s + "\napply 1")
        .thenApply(s -> s + "\napply 2");

    // when:
    final String result = applied.get();

    // then:
    assertThat(result, is("inner done\napply 1\napply 2"));
  }

  @Test
  public void shouldHandleMultipleNestedComposes() throws InterruptedException, ExecutionException {
    // given:
    inner.complete("inner done");
    final ResponsiveFuture<String> composed = responsiveFuture
        .thenCompose(s -> nested1)
        .thenCompose(s -> nested2);
    nested1.complete("nested 1");
    nested2.complete("nested 2");

    // when:
    final String result = composed.get();

    // then:
    assertThat(result, is("nested 2"));
  }
}