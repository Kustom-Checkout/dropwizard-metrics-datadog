package org.coursera.metrics.datadog.transport;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hc.core5.http.ParseException;
import org.coursera.metrics.datadog.model.DatadogCounter;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.model.DatadogRate;
import org.coursera.metrics.serializer.JsonSerializer;
import org.coursera.metrics.serializer.Serializer;

import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.DeflaterInputStream;

/**
 * Uses the datadog http webservice to push metrics.
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */
public class HttpTransport implements Transport {

  private static final Logger LOG = LoggerFactory.getLogger(HttpTransport.class);

  private static final String SERIES_URL = "https://api.datadoghq.eu/api/v2/series";
  private final String apiKey;
  private final int connectTimeout;     // in milliseconds
  private final int responseTimeout;      // in milliseconds
  private final HttpHost proxy;
  private final Executor executor;
  private final boolean useCompression;

  private HttpTransport(String apiKey,
                        int connectTimeout,
                        int responseTimeout,
                        HttpHost proxy,
                        Executor executor,
                        boolean useCompression) {
    this.apiKey = apiKey;
    this.connectTimeout = connectTimeout;
    this.responseTimeout = responseTimeout;
    this.proxy = proxy;
    this.useCompression = useCompression;
    this.executor = Objects.requireNonNullElseGet(executor, Executor::newInstance);
  }

  public static class Builder {
    String apiKey;
    int connectTimeout = 5000;
    int responseTimeout = 5000;
    HttpHost proxy;
    Executor executor;
    boolean useCompression = false;

    public Builder withApiKey(String key) {
      this.apiKey = key;
      return this;
    }

    public Builder withConnectTimeout(int milliseconds) {
      this.connectTimeout = milliseconds;
      return this;
    }

    public Builder withResponseTimeout(int milliseconds) {
      this.responseTimeout = milliseconds;
      return this;
    }

    public Builder withProxy(String proxyHost, int proxyPort) {
      this.proxy = new HttpHost(proxyHost, proxyPort);
      return this;
    }

    public Builder withExecutor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public Builder withCompression(boolean compression) {
      this.useCompression = compression;
      return this;
    }

    public HttpTransport build() {
      return new HttpTransport(apiKey, connectTimeout, responseTimeout, proxy, executor, useCompression);
    }
  }

  @Override
  public Request prepare() throws IOException {
    return new HttpRequest(this);
  }

  @Override
  public void close() throws IOException {
  }

  public static class HttpRequest implements Transport.Request {
    protected final Serializer serializer;

    protected final HttpTransport transport;

    public HttpRequest(HttpTransport transport) throws IOException {
      this.transport = transport;
      serializer = new JsonSerializer();
      serializer.startObject();
    }

    @Override
    public void addGauge(DatadogGauge gauge) throws IOException {
      serializer.appendGauge(gauge);
    }

    @Override
    public void addCounter(DatadogCounter counter) throws IOException {
      serializer.appendCounter(counter);
    }

    @Override
    public void addRate(DatadogRate rate) throws IOException {
      serializer.appendRate(rate);
    }

    @Override
    public void send() throws Exception {
      serializer.endObject();
      var postBody = serializer.getAsString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending HTTP POST request to proxy {}, uncompressed POST body length is: {}", transport.proxy, postBody.length());
        LOG.debug("Uncompressed POST body is: \n{}", postBody);
      }
      var start = System.currentTimeMillis();
      var request = org.apache.hc.client5.http.fluent.Request.post(SERIES_URL)
              .useExpectContinue()
              .addHeader("DD-API-KEY", transport.apiKey)
              .connectTimeout(Timeout.ofMicroseconds(transport.connectTimeout))
              .responseTimeout(Timeout.ofMicroseconds(transport.responseTimeout));

      if (transport.useCompression) {
        request
                .addHeader("Content-Encoding", "deflate")
                .addHeader("Content-MD5", DigestUtils.md5Hex(postBody))
                .bodyStream(deflated(postBody), ContentType.APPLICATION_JSON);
      } else {
        request.bodyString(postBody, ContentType.APPLICATION_JSON);
      }

      if (transport.proxy != null) {
        request.viaProxy(transport.proxy);
      }

      try {
        var response = transport.executor.execute(request);

        var elapsed = System.currentTimeMillis() - start;

        if (LOG.isWarnEnabled()) {
          response.handleResponse(new HttpClientResponseHandler<Void>() {
            @Override
            public Void handleResponse(ClassicHttpResponse classicHttpResponse) throws HttpException, IOException {
              var statusCode = classicHttpResponse.getCode();
              if (statusCode >= 400) {
                LOG.warn(getLogMessage("Failure sending metrics to Datadog: ", classicHttpResponse));
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(getLogMessage("Sent metrics to Datadog: ", classicHttpResponse));
                }
              }
              return null;
            }

            private String getLogMessage(String headline, ClassicHttpResponse response) throws IOException, ParseException {
              var sb = new StringBuilder();
              sb.append(headline);
              sb.append("\n");
              sb.append("  Timing: ").append(elapsed).append(" ms\n");
              sb.append("  Status: ").append(response.getCode()).append("\n");

              var content = EntityUtils.toString(response.getEntity(), "UTF-8");
              sb.append("  Content: ").append(content);
              return sb.toString();
            }

          });
        } else {
          response.discardContent();
        }
      } catch (IOException e) {
        LOG.warn("Failed to send metrics to Datadog: proxy: {}, error: {}", transport.proxy, e.getMessage());
      }
    }

    private static InputStream deflated(String str) throws IOException {
      if (str == null || str.isEmpty()) {
        return new ByteArrayInputStream(new byte[0]);
      }
      var inputStream = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
      return new DeflaterInputStream(inputStream) {
        @Override
        public void close() throws IOException {
          if (LOG.isDebugEnabled()) {
            var sb = new StringBuilder();
            var bytesWritten = def.getBytesWritten();
            var bytesRead = def.getBytesRead();
            sb.append("POST body length compressed / uncompressed / compression ratio: ");
            sb.append(bytesWritten);
            sb.append(" / ");
            sb.append(bytesRead);
            sb.append(" / ");
            sb.append(String.format( "%.2f", bytesRead / (double)bytesWritten));
            LOG.debug(sb.toString());
          }
          super.close();
        }
      };
    }
  }
}
