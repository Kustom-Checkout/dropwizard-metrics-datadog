package org.coursera.metrics.datadog;

import java.util.List;

/**
 * An implementation of this interface can be used to pass a callback to the builder of
 * {@link org.coursera.metrics.datadog.DatadogReporter DatadogReporter}, so that DatadogReporter
 * can use dynamic tags
 */
public interface DynamicTagsCallback {
  /**
   *
   * @return dynamic tags that will merge into the static tags. Dynamic tags will overwrite
   * static tags with the same key
   */
  List<String> getTags();
}
