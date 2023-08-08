package dev.responsive.k8s.crd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DemoPolicy {
  public static class ProcessingRateDiagnoser {
    private final int rate;
    // don't use OptionalInt here. The CRD schema generator only handles Optional transparently
    private final Optional<Integer> windowMs;

    public int getRate() {
      return rate;
    }

    public Optional<Integer> getWindowMs() {
      return windowMs;
    }

    public ProcessingRateDiagnoser(
        @JsonProperty("rate") final int rate,
        @JsonProperty("windowMs") final Optional<Integer> windowMs) {
      this.rate = rate;
      this.windowMs = Objects.requireNonNull(windowMs);
    }
  }

  public static class Diagnoser {
    public enum Type {
      PROCESSING_RATE_SCALE_UP,
      PROCESSING_RATE_SCALE_DOWN,
      LAG_SCALE_UP
    }

    private final Type type;
    private final Optional<ProcessingRateDiagnoser> processingRateScaleUp;
    private final Optional<ProcessingRateDiagnoser> processingRateScaleDown;

    @JsonCreator
    public Diagnoser(
        @JsonProperty("type") final Type strategy,
        @JsonProperty("processingRateScaleUp")
            final Optional<ProcessingRateDiagnoser> processingRateScaleUp,
        @JsonProperty("processingRateScaleDown")
            final Optional<ProcessingRateDiagnoser> processingRateScaleDown) {
      this.type = strategy;
      this.processingRateScaleDown = Objects.requireNonNull(processingRateScaleDown);
      this.processingRateScaleUp = Objects.requireNonNull(processingRateScaleUp);
    }

    public static Diagnoser lag() {
      return new Diagnoser(Type.LAG_SCALE_UP, Optional.empty(), Optional.empty());
    }

    public static Diagnoser processingRateScaleUp(final ProcessingRateDiagnoser diagnoser) {
      return new Diagnoser(Type.PROCESSING_RATE_SCALE_UP, Optional.of(diagnoser), Optional.empty());
    }

    public static Diagnoser processingRateScaleDown(final ProcessingRateDiagnoser diagnoser) {
      return new Diagnoser(
          Type.PROCESSING_RATE_SCALE_DOWN, Optional.empty(), Optional.of(diagnoser));
    }

    public Type getType() {
      return type;
    }

    public Optional<ProcessingRateDiagnoser> getProcessingRateScaleUp() {
      return processingRateScaleUp;
    }

    public Optional<ProcessingRateDiagnoser> getProcessingRateScaleDown() {
      return processingRateScaleDown;
    }

    private void validate() {
      Objects.requireNonNull(type);
      switch (type) {
        case PROCESSING_RATE_SCALE_UP:
          CrdUtils.validatePresent(processingRateScaleUp, "processingRateScaleUp");
          break;
        case PROCESSING_RATE_SCALE_DOWN:
          CrdUtils.validatePresent(processingRateScaleDown, "processingRateScaleDown");
          break;
        default:
          break;
      }
    }
  }

  private final int maxReplicas;
  private final int minReplicas;
  private final Optional<List<Diagnoser>> diagnosers;

  @JsonCreator
  public DemoPolicy(
      @JsonProperty("maxReplicas") final int maxReplicas,
      @JsonProperty("minReplicas") final int minReplicas,
      @JsonProperty("diagnosers") final Optional<List<Diagnoser>> diagnosers) {
    this.maxReplicas = maxReplicas;
    this.minReplicas = minReplicas;
    this.diagnosers = Objects.requireNonNull(diagnosers);
  }

  void validate() {
    diagnosers.ifPresent(ds -> ds.forEach(Diagnoser::validate));
  }

  public int getMaxReplicas() {
    return maxReplicas;
  }

  public int getMinReplicas() {
    return minReplicas;
  }

  public Optional<List<Diagnoser>> getDiagnosers() {
    return diagnosers;
  }
}
