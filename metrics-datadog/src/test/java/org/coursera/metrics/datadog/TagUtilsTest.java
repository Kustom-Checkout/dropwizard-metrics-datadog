package org.coursera.metrics.datadog;

import org.junit.Test;

import java.util.ArrayList;
import java.util.TreeSet;


public class TagUtilsTest {

  @Test
  public void mergeTagsWithoutDupKey() throws Exception {
    var tags1 = new ArrayList<String>();
    tags1.add("key1:v1");
    tags1.add("key2:v2");
    var tags2 = new ArrayList<String>();
    tags2.add("key3:v3");
    tags2.add("key4:v4");


    var expected = new ArrayList<String>();
    expected.addAll(tags1);
    expected.addAll(tags2);
    assert(new TreeSet<>(TagUtils.mergeTags(tags1, tags2)).equals(
            new TreeSet<>(expected)));
  }

  @Test
  public void mergeTagsWithDupKey() throws Exception {
    var tags1 = new ArrayList<String>();
    tags1.add("key1:v1");
    tags1.add("key2:v2");
    var tags2 = new ArrayList<String>();
    tags2.add("key2:v3");
    tags2.add("key4:v4");


    var expected = new ArrayList<String>();
    expected.add("key1:v1");
    expected.addAll(tags2);
    assert(new TreeSet<>(TagUtils.mergeTags(tags1, tags2)).equals(
            new TreeSet<>(expected)));
  }
}
