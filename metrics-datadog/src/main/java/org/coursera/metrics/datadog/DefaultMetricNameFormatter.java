package org.coursera.metrics.datadog;

public class DefaultMetricNameFormatter implements MetricNameFormatter {

  public String format(String name, String... path) {
    var sb = new StringBuilder();

    var metricParts = name.split("\\[");
    sb.append(metricParts[0]);

    for (var part : path) {
        sb.append('.').append(part);
    }

    for (var i = 1; i < metricParts.length; i++) {
        sb.append('[').append(metricParts[i]);
    }
    return sb.toString();
  }
}