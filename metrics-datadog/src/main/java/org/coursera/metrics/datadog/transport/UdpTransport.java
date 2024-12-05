package org.coursera.metrics.datadog.transport;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import org.coursera.metrics.datadog.model.DatadogCounter;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.model.DatadogRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Uses dogstatsd UDP protocol to push metrics to datadog. Note that datadog doesn't support
 * time in the UDP protocol. So all metrics are against current time.
 * <p/>
 * Also dogstatsd UDP doesn't support batching of metrics, so it pushes metrics as it receives
 * rather than batching.
 *
 * @see <a href="http://docs.datadoghq.com/guides/dogstatsd">dogstatsd</a>
 */
public class UdpTransport implements Transport {

  private static final Logger LOG = LoggerFactory.getLogger(UdpTransport.class);
  private final StatsDClient statsd;
  private final Map<String, Long> lastSeenCounters = new HashMap<>();

  private UdpTransport(String prefix, String statsdHost, int port, boolean isRetryingLookup, String[] globalTags) {
    var socketAddressCallable = isRetryingLookup
            ? volatileAddressResolver(statsdHost, port)
            : staticAddressResolver(statsdHost, port);

    statsd = new NonBlockingStatsDClientBuilder()
            .prefix(prefix)
            .queueSize(Integer.MAX_VALUE)
            .constantTags(globalTags)
            .errorHandler(e ->
              LOG.error("statsdHost: {}, port: {}, isRetryingLookup {}, errorMessage: {}", statsdHost, port, isRetryingLookup, e.getMessage())
            )
            .addressLookup(socketAddressCallable)
            .build();
    LOG.info("Created UdpTransport {} with statsdHost: {}, port: {}, isRetryingLookup: {}", statsd, statsdHost, port, isRetryingLookup);
  }

  @Override
  public void close() throws IOException {
    statsd.stop();
  }

  public static class Builder {
    String prefix = null;
    String statsdHost = "localhost";
    int port = 8125;
    boolean isLookupRetrying = false;

    public Builder withPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder withStatsdHost(String statsdHost) {
      this.statsdHost = statsdHost;
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    public Builder withRetryingLookup(boolean isRetrying) {
      this.isLookupRetrying = isRetrying;
      return this;
    }

    public UdpTransport build() {
      return new UdpTransport(prefix, statsdHost, port, isLookupRetrying, new String[0]);
    }
  }

  @Override
  public Request prepare() throws IOException {
    return new DogstatsdRequest(statsd, lastSeenCounters);
  }

  public static class DogstatsdRequest implements Transport.Request {
    private final StatsDClient statsdClient;
    private final Map<String, Long> lastSeenCounters;

    public DogstatsdRequest(StatsDClient statsdClient, Map<String, Long> lastSeenCounters) {
      this.statsdClient = statsdClient;
      this.lastSeenCounters = lastSeenCounters;
    }

    /**
     * statsd has no notion of batch request, so gauges are pushed as they are received
     */
    @Override
    public void addGauge(DatadogGauge gauge) {
      if (gauge.getPoints().size() > 1) {
          LOG.debug("Gauge {} has more than one data point, will pick the first point only", gauge.getMetric());
      }
      var value = gauge.getPoints().getFirst().get(1).doubleValue();
      var tags = gauge.getTags().toArray(new String[0]);
      statsdClient.gauge(gauge.getMetric(), value, tags);
    }

    /**
     * statsd has no notion of batch request, so counters are pushed as they are received
     */
    @Override
    public void addCounter(DatadogCounter counter) {
      if (counter.getPoints().size() > 1) {
          LOG.debug("Counter {} has more than one data point, will pick the first point only", counter.getMetric());
      }
      var value = counter.getPoints().getFirst().get(1).longValue();
      var tags = counter.getTags().toArray(new String[0]);
      var sb = new StringBuilder();
      for (var i = tags.length - 1; i >= 0; i--) {
        sb.append(tags[i]);
        if (i > 0) {
          sb.append(",");
        }
      }

      var metric = counter.getMetric();
      var finalMetricsSeenName = metric + ":" + sb;
      var finalValue = value;
      if (lastSeenCounters.containsKey(finalMetricsSeenName)) {
        // If we've seen this counter before then calculate the difference
        // by subtracting the new value from the old. StatsD expects a relative
        // counter, not an absolute!
        finalValue = Math.max(0, value - lastSeenCounters.get(finalMetricsSeenName));
      }
      // Store the last value we saw so that the next addCounter call can make
      // the proper relative value
      lastSeenCounters.put(finalMetricsSeenName, value);

      statsdClient.count(metric, finalValue, tags);
    }

    @Override
    public void addRate(DatadogRate rate) throws IOException {

    }

    /**
     * For statsd the metrics are pushed as they are received. So there is nothing do in send.
     */
    @Override
    public void send() {
    }
  }

  // Visible for testing.
  static Callable<SocketAddress> staticAddressResolver(final String host, final int port) {
    try {
      return NonBlockingStatsDClientBuilder.staticAddressResolution(host, port);
    } catch (final Exception e) {
      LOG.error("Error during constructing statsd address resolver.", e);
      throw new RuntimeException(e);
    }
  }

  // Visible for testing.
  static Callable<SocketAddress> volatileAddressResolver(final String host, final int port) {
    return NonBlockingStatsDClientBuilder.volatileAddressResolution(host, port);
  }
}
