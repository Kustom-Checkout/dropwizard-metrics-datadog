package org.coursera.metrics.datadog;

public interface MetricNameFormatter {

  String format(String name, String... path);
}
