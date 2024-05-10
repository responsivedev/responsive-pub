package dev.responsive.examples.e2etest;

public class Params {
  public static final String NAME = System.getenv().getOrDefault("TEST_NAME", "e2e");
  public static final String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "input");
  public static final String OUTPUT_TOPIC = System.getenv().getOrDefault("OUTPUT_TOPIC", "output");
  public static final int PARTITIONS
      = Integer.parseInt(System.getenv().getOrDefault("PARTITIONS", "8"));
  public static final int EXCEPTION_INJECT_THRESHOLD
      = Integer.parseInt(System.getenv().getOrDefault("EXCEPTION_INJECT_THRESHOLD", "1"));
  public static final int NUM_KEYS
      = Integer.parseInt(System.getenv().getOrDefault("NUM_KEYS", "100"));
  public static final int MAX_OUTSTANDING
      = Integer.parseInt(System.getenv().getOrDefault("MAX_OUTSTANDING", "10000"));
  public static final int RECEIVE_THRESHOLD
      = Integer.parseInt(System.getenv().getOrDefault("RECEIVE_THRESHOLD", "300"));
  public static final String MODE = System.getenv().getOrDefault("E2E_APP_MODE", "APPLICATION");
}
