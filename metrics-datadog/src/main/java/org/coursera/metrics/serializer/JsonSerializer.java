package org.coursera.metrics.serializer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.coursera.metrics.datadog.model.DatadogCounter;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.model.DatadogSeries;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Serialize datadog time series object into json
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */
public class JsonSerializer implements Serializer {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(JSON_FACTORY);

  private JsonGenerator jsonOut;
  private ByteArrayOutputStream outputStream;

  public void startObject() throws IOException {
    outputStream = new ByteArrayOutputStream(2048);
    jsonOut = JSON_FACTORY.createGenerator(outputStream);
    jsonOut.writeStartObject();
    jsonOut.writeArrayFieldStart("series");
  }

  public void appendGauge(DatadogGauge gauge) throws IOException {
    MAPPER.writeValue(jsonOut, new MetricSeries(gauge));
  }

  public void appendCounter(DatadogCounter counter) throws IOException {
    MAPPER.writeValue(jsonOut, new MetricSeries(counter));
  }

  private record MetricSeries(String metric, List<MetricPoint> points, List<String> tags, int type, List<MetricResource> resources) {
    MetricSeries(DatadogSeries<?> datadogSeries) {
      this(datadogSeries.getMetric(),
              datadogSeries.getPoints().stream()
                      .map(p -> new MetricPoint(p.getFirst().longValue(), p.getLast().doubleValue())).toList(),
              datadogSeries.getTags(),
              MetricType.from(datadogSeries),
              List.of(new MetricResource(datadogSeries.getHost(), "host")));
    }

    private enum MetricType {
      UNSPECIFIED(0),
      COUNT(1),
      RATE(2),
      GAUGE(3);

      final int value;

      MetricType(int value) {
        this.value = value;
      }

      public static int from(DatadogSeries<?> datadogSeries) {
        for (var type : values()) {
          if (type.name().equalsIgnoreCase(datadogSeries.getType())) {
            return type.value;
          }
        }
        return 0;
      }
    }

    private record MetricPoint(long timestamp, double value) {}

    private record MetricResource(String name, String type) {}
  }

  public void endObject() throws IOException {
    jsonOut.writeEndArray();
    jsonOut.writeEndObject();
    jsonOut.flush();
    outputStream.close();
  }

  public String getAsString() throws UnsupportedEncodingException {
    return outputStream.toString(StandardCharsets.UTF_8);
  }
}
