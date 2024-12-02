package org.coursera.metrics.datadog;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import org.fest.assertions.api.Assertions;
import org.junit.Test;

public class DefaultMetricNameFormatterFactoryTest {
  @Test
  public void isDiscoverable() {
    Assertions
            .assertThat(new DiscoverableSubtypeResolver().getDiscoveredSubtypes())
            .contains(DefaultMetricNameFormatterFactory.class);
  }

  @Test
  public void testBuild() {
    Assertions
            .assertThat(new DefaultMetricNameFormatterFactory().build())
            .isInstanceOf(DefaultMetricNameFormatter.class);
  }
}
