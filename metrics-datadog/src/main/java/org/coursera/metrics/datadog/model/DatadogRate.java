package org.coursera.metrics.datadog.model;

import java.util.List;

public class DatadogRate extends DatadogSeries<Long> {

  public DatadogRate(String name, Long count, Long epoch, String host, List<String> additionalTags) {
    super(name, count, epoch, host, additionalTags);
  }

  public String getType() {
    return "rate";
  }
}