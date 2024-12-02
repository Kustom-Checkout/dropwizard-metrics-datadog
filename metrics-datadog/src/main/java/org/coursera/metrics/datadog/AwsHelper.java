package org.coursera.metrics.datadog;

import org.apache.hc.client5.http.fluent.Request;

import java.io.IOException;

public class AwsHelper {

  public static final String url = "http://169.254.169.254/latest/meta-data/instance-id";

  public static String getEc2InstanceId() throws IOException {
    try {
      return Request.get(url).execute().returnContent().asString();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}