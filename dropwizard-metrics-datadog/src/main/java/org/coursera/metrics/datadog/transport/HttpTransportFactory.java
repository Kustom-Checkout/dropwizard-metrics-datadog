package org.coursera.metrics.datadog.transport;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.util.Duration;

import javax.validation.constraints.NotNull;

@JsonTypeName("http")
public class HttpTransportFactory implements AbstractTransportFactory {

  @NotNull
  @JsonProperty
  private String apiKey = null;

  @JsonProperty
  private Duration connectTimeout = Duration.seconds(5);

  @JsonProperty
  private Duration responseTimeout = Duration.seconds(5);

  @JsonProperty
  private int proxyPort;

  @JsonProperty
  private String proxyHost;

  @JsonProperty
  private Boolean compression;

  public HttpTransport build() {
    var builder = new HttpTransport.Builder()
        .withApiKey(apiKey)
        .withCompression(Boolean.TRUE.equals(compression))
        .withConnectTimeout((int) connectTimeout.toMilliseconds())
        .withResponseTimeout((int) responseTimeout.toMilliseconds());

    if (proxyPort != 0 || proxyHost != null) {
      if (proxyPort == 0 || proxyHost == null) {
        throw new IllegalStateException("must set both proxyPort and proxyHost or neither");
      }

      builder = builder.withProxy(proxyHost, proxyPort);
    }

    return builder.build();
  }
}
