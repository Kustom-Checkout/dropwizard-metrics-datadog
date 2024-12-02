package org.coursera.metrics.datadog;

import java.lang.*;
import java.lang.Object;
import java.lang.Override;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class TaggedName {
  private static final Pattern tagPattern = Pattern
      .compile("([\\w.-]+)\\[([\\w\\W]+)]");

  private final String metricName;
  private final List<String> encodedTags;

  private TaggedName(String metricName, List<String> encodedTags) {
    this.metricName = metricName;
    this.encodedTags = encodedTags;
  }

  public String getMetricName() {
    return metricName;
  }

  public List<String> getEncodedTags() {
    return encodedTags;
  }

  public String encode() {
    if (!encodedTags.isEmpty()) {
      var sb = new StringBuilder(this.metricName);
      sb.append('[');
      var prefix = "";
      for (var encodedTag : encodedTags) {
        sb.append(prefix);
        sb.append(encodedTag);
        prefix = ",";
      }
      sb.append(']');
      return sb.toString();
    } else {
      return this.metricName;
    }
  }

  public static TaggedName decode(String encodedTaggedName) {
    var builder = new TaggedNameBuilder();

    var matcher = tagPattern.matcher(encodedTaggedName);
    if (matcher.find() && matcher.groupCount() == 2) {
      builder.metricName(matcher.group(1));
      for (var t : matcher.group(2).split(",")) {
        builder.addTag(t);
      }
    } else {
      builder.metricName(encodedTaggedName);
    }

    return builder.build();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    var that = (TaggedName) o;

    if (!Objects.equals(metricName, that.metricName)) return false;
    return Objects.equals(encodedTags, that.encodedTags);
  }

  @Override
  public int hashCode() {
    int result = metricName != null ? metricName.hashCode() : 0;
    result = 31 * result + (encodedTags != null ? encodedTags.hashCode() : 0);
    return result;
  }


  public static class TaggedNameBuilder {
    private String metricName;
    private final List<String> encodedTags = new ArrayList<>();

    public TaggedNameBuilder metricName(String metricName) {
      this.metricName = metricName;
      return this;
    }

    public TaggedNameBuilder addTag(String key, String val) {
      assertNonEmpty(key, "tagKey");
      encodedTags.add(key + ':' + val);
      return this;
    }

    public TaggedNameBuilder addTag(String encodedTag) {
      assertNonEmpty(encodedTag, "encodedTag");
      encodedTags.add(encodedTag);
      return this;
    }

    private void assertNonEmpty(String s, String field) {
      if (s == null || s.trim().isEmpty()) {
        throw new IllegalArgumentException((field + " must be defined"));
      }
    }

    public TaggedName build() {
      assertNonEmpty(this.metricName, "metricName");

      return new TaggedName(this.metricName, this.encodedTags);
    }
  }
}
