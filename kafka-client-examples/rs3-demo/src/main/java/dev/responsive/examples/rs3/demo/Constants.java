package dev.responsive.examples.rs3.demo;

public class Constants {
  public static final String SUMMARIZED_ORDERS_TOPIC = "summarized-orders";
  public static int NUM_PARTITIONS = 8;
  public static String ORDERS = "orders";
  public static final int RECORDS_PER_SECOND = Integer.parseInt(
      System.getenv().getOrDefault(
          "RECORDS_PER_SECOND",
          "100"
      )
  );
  public static final String GENERATOR = "GENERATOR";
  public static final String MODE = System.getenv().getOrDefault(
      "MODE",
      "APP"
  );
}
