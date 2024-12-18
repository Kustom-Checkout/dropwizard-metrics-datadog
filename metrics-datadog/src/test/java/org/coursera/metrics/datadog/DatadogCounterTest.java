package org.coursera.metrics.datadog;

import org.coursera.metrics.datadog.model.DatadogCounter;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class DatadogCounterTest {

  @Test
  public void testSplitNameAndTags() {
    var tags = new ArrayList<String>();
    tags.add("env:prod");
    tags.add("version:1.0.0");
    var counter = new DatadogCounter(
        "test[tag1:value1,tag2:value2,tag3:value3]", 1L, 1234L, "Test Host", tags);
    var allTags = counter.getTags();

    assertEquals(5, allTags.size());
    assertEquals("tag1:value1", allTags.get(0));
    assertEquals("tag2:value2", allTags.get(1));
    assertEquals("tag3:value3", allTags.get(2));
    assertEquals("env:prod", allTags.get(3));
    assertEquals("version:1.0.0", allTags.get(4));
  }

}
