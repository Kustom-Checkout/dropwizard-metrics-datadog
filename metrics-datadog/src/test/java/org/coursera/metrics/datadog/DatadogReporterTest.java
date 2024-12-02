package org.coursera.metrics.datadog;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.coursera.metrics.datadog.DatadogReporter.Expansion;
import org.coursera.metrics.datadog.model.DatadogGauge;
import org.coursera.metrics.datadog.transport.Transport;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class DatadogReporterTest {
    private static final String HOST = "hostname";
    private static final String PREFIX = "testprefix";
    private final long timestamp = 1000198;
    private final Clock clock = mock(Clock.class);
    private final Transport transport = mock(Transport.class);
    private final Transport.Request request = mock(Transport.Request.class);
    private final DynamicTagsCallback callback = mock(DynamicTagsCallback.class);
    private MetricRegistry metricsRegistry;
    private DatadogReporter reporter;
    private DatadogReporter reporterWithPrefix;
    private DatadogReporter reporterWithCallback;
    private List<String> tags;

    @Before
    public void setUp() throws IOException {
        when(clock.getTime()).thenReturn(timestamp * 1000);
        when(transport.prepare()).thenReturn(request);
        metricsRegistry = new MetricRegistry();
        tags = new ArrayList<>();
        tags.add("env:prod");
        tags.add("version:1.0.0");
        reporter = DatadogReporter
                .forRegistry(metricsRegistry)
                .withHost(HOST)
                .withClock(clock)
                .withTags(tags)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .withTransport(transport)
                .build();

        reporterWithPrefix = DatadogReporter
                .forRegistry(metricsRegistry)
                .withHost(HOST)
                .withPrefix(PREFIX)
                .withClock(clock)
                .withTags(tags)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .withTransport(transport)
                .build();

        reporterWithCallback = DatadogReporter
                .forRegistry(metricsRegistry)
                .withHost(HOST)
                .withClock(clock)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .withTransport(transport)
                .withDynamicTagCallback(callback)
                .build();

    }

    @Test
    public void reportsByteGaugeValues() throws Exception {
        var gauge = gauge((byte) 1);

        reporter.report(map("gauge", gauge),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper((byte) 1, tags);
    }

    @Test
    public void reportsShortGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge((short) 1)),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper((short) 1, tags);
    }

    @Test
    public void reportsIntegerGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1)),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper(1, tags);
    }

    @Test
    public void reportsLongGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1L)),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper(1L, tags);
    }

    @Test
    public void reportsFloatGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1.1f)),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper(1.1f, tags);
    }

    @Test
    public void reportsDoubleGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1.1)),
                map(),
                map(),
                map(),
                map());

        gaugeTestHelper(1.1, tags);
    }

    @Test
    public void reportHandlesGaugeMetricExceptions() throws Exception {
        var gauge = mock(Gauge.class);
        when(gauge.getValue()).thenThrow(new IllegalArgumentException("error occurred during retrieving value"));
        var counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(100L);

        reporter.report(map("gauge", gauge),
                map("counter", counter),
                map(),
                map(),
                map());

        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("counter", 100L, timestamp, HOST, tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }

    @Test
    public void reportsCounters() throws Exception {
        var counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(100L);

        reporter.report(map(),
                map("counter", counter),
                map(),
                map(),
                map());

        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("counter", 100L, timestamp, HOST, tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }

    @Test
    public void reportsHistograms() throws Exception {
        var histogram = mock(Histogram.class);
        when(histogram.getCount()).thenReturn(1L);

        var snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(2L);
        when(snapshot.getMean()).thenReturn(3.0);
        when(snapshot.getMin()).thenReturn(4L);
        when(snapshot.getStdDev()).thenReturn(5.0);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(histogram.getSnapshot()).thenReturn(snapshot);

        reporter.report(map(),
                map(),
                map("histogram", histogram),
                map(),
                map());

        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.count", 1L, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.max", 2L, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.mean", 3.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.min", 4L, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.stddev", 5.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.median", 6.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.p75", 7.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.p95", 8.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.p98", 9.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.p99", 10.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("histogram.p999", 11.0, timestamp, HOST, tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }

    @Test
    public void reportsMeters() throws Exception {
        var meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);
        when(meter.getMeanRate()).thenReturn(5.0);

        reporter.report(map(),
                map(),
                map(),
                map("meter", meter),
                map());

        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("meter.count", 1L, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("meter.1MinuteRate", 2.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("meter.5MinuteRate", 3.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("meter.15MinuteRate", 4.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("meter.meanRate", 5.0, timestamp, HOST, tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }

    @Test
    public void reportsTimers() throws Exception {
        var timer = mock(Timer.class);
        when(timer.getCount()).thenReturn(1L);
        when(timer.getMeanRate()).thenReturn(2.0);
        when(timer.getOneMinuteRate()).thenReturn(3.0);
        when(timer.getFiveMinuteRate()).thenReturn(4.0);
        when(timer.getFifteenMinuteRate()).thenReturn(5.0);

        var snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(TimeUnit.MILLISECONDS.toNanos(100));
        when(snapshot.getMean())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(200));
        when(snapshot.getMin())
                .thenReturn(TimeUnit.MILLISECONDS.toNanos(300));
        when(snapshot.getStdDev())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(400));
        when(snapshot.getMedian())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
        when(snapshot.get75thPercentile())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(600));
        when(snapshot.get95thPercentile())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));
        when(snapshot.get98thPercentile())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(800));
        when(snapshot.get99thPercentile())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(900));
        when(snapshot.get999thPercentile())
                .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(1000));

        when(timer.getSnapshot()).thenReturn(snapshot);

        reporter.report(map(),
                map(),
                map(),
                map(),
                map("timer", timer));

        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("timer.max", 100.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.mean", 200.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.min", 300.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.stddev", 400.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.median", 500.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.p75", 600.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.p95", 700.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.p98", 800.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.p99", 900.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.p999", 1000.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.count", 1L, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.1MinuteRate", 3.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.5MinuteRate", 4.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.15MinuteRate", 5.0, timestamp, HOST, tags));
        inOrder.verify(request).addGauge(new DatadogGauge("timer.meanRate", 2.0, timestamp, HOST, tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }

    @Test
    public void reportsWithPrefix() throws Exception {

        var counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(100L);

        reporterWithPrefix.report(map(),
                map("counter", counter),
                map(),
                map(),
                map());

        verify(request).addGauge(new DatadogGauge("testprefix.counter", 100L, timestamp, HOST, tags));
        verify(request, never()).addGauge(new DatadogGauge("counter", 100L, timestamp, HOST, tags));
    }

    @Test
    public void reportsWithCallback() throws Exception {
        List<String> dynamicTags = new ArrayList<String>();
        dynamicTags.add("status:active");
        dynamicTags.add("speed:29");

        when(callback.getTags()).thenReturn(dynamicTags);

        var counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(100L);

        reporterWithCallback.report(map(),
                map("counter", counter),
                map(),
                map(),
                map());

        verify(request).addGauge(new DatadogGauge("counter", 100L, timestamp, HOST, dynamicTags));
    }

    @Test
    public void reportsWithMetricNameFormatter() throws Exception {
        var gauge = mock(Gauge.class);
        var counter = mock(Counter.class);
        when(gauge.getValue()).thenReturn(100L);
        when(counter.getCount()).thenReturn(100L);

        try (var reporterWithMetricNameFormatter = DatadogReporter
                .forRegistry(metricsRegistry)
                .withHost(HOST)
                .withClock(clock)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .withTransport(transport)
                .withMetricNameFormatter(new DefaultMetricNameFormatter() {

                    public String format(String name, String... path) {
                        return "metric_name_formatter." + super.format(name, path);
                    }

                })
                .build()) {

            reporterWithMetricNameFormatter.report(
                    map("gauge", gauge),
                    map("counter", counter),
                    map(),
                    map(),
                    map()
            );

            verify(request).addGauge(
                    new DatadogGauge("metric_name_formatter.gauge", 100L, timestamp, HOST, null)
            );
            verify(request).addGauge(
                    new DatadogGauge("metric_name_formatter.counter", 100L, timestamp, HOST, null)
            );
        }
    }

    @Test
    public void reportsWithFilter() throws Exception {
        var counter = metricsRegistry.counter("my.metric.counter");
        counter.inc(123);
        var unwantedCounter = metricsRegistry.counter("counter");
        unwantedCounter.inc(456);

        try (var reporterWithFilter =
                     DatadogReporter
                             .forRegistry(metricsRegistry)
                             .withHost(HOST)
                             .withClock(clock)
                             .withTags(tags)
                             .filter(new NameMetricFilter("my.metric"))
                             .withTransport(transport)
                             .build()) {

            reporterWithFilter.report();

            var inOrder = inOrder(transport, request);
            inOrder.verify(transport).prepare();
            inOrder.verify(request).addGauge(new DatadogGauge("my.metric.counter",
                    123L,
                    timestamp,
                    HOST,
                    tags));
            inOrder.verify(request, never()).addGauge(new DatadogGauge("counter",
                    123L,
                    timestamp,
                    HOST,
                    tags));
            inOrder.verify(request).send();

            verify(transport).prepare();
            verify(request).send();
            verifyNoMoreInteractions(transport, request);
        }
    }

    @Test
    public void reportsWithTagged() throws Exception {
        var meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);
        when(meter.getMeanRate()).thenReturn(5.0);
        metricsRegistry.meter(MetricRegistry.name(String.class, "meter[with,tags]"));

        reporter.report();

        var inOrder = inOrder(transport, request);
        inOrder.verify(request)
                .addGauge(new DatadogGauge("java.lang.String.meter.count[with,tags]",
                        0L,
                        timestamp,
                        HOST,
                        tags));
        inOrder.verify(request)
                .addGauge(new DatadogGauge("java.lang.String.meter.1MinuteRate[with,tags]",
                        0.0,
                        timestamp,
                        HOST,
                        tags));
        inOrder.verify(request)
                .addGauge(new DatadogGauge("java.lang.String.meter.5MinuteRate[with,tags]",
                        0.0,
                        timestamp,
                        HOST,
                        tags));
        inOrder.verify(request)
                .addGauge(new DatadogGauge("java.lang.String.meter.15MinuteRate[with,tags]",
                        0.0,
                        timestamp,
                        HOST,
                        tags));
        inOrder.verify(request)
                .addGauge(new DatadogGauge("java.lang.String.meter.meanRate[with,tags]",
                        0.0,
                        timestamp,
                        HOST,
                        tags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }


    @Test
    public void reportsWithExpansions() throws Exception {
        try (var reporterWithExpansions = DatadogReporter
                .forRegistry(metricsRegistry)
                .withHost(HOST)
                .withClock(clock)
                .withTags(tags)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .withTransport(transport)
                .withExpansions(EnumSet.of(Expansion.COUNT, Expansion.P95, Expansion.MEDIAN, Expansion.RATE_1_MINUTE))
                .build()) {

            var timer = mock(Timer.class);
            when(timer.getCount()).thenReturn(1L);
            when(timer.getOneMinuteRate()).thenReturn(3.0);

            var snapshot = mock(Snapshot.class);
            when(snapshot.getMedian())
                    .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
            when(snapshot.get95thPercentile())
                    .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));

            when(timer.getSnapshot()).thenReturn(snapshot);

            reporterWithExpansions.report(map(),
                    map(),
                    map(),
                    map(),
                    map("timer", timer));

            var inOrder = inOrder(transport, request);
            inOrder.verify(transport).prepare();
            inOrder.verify(request).addGauge(new DatadogGauge("timer.median", 500.0, timestamp, HOST, tags));
            inOrder.verify(request).addGauge(new DatadogGauge("timer.p95", 700.0, timestamp, HOST, tags));
            inOrder.verify(request).addGauge(new DatadogGauge("timer.count", 1L, timestamp, HOST, tags));
            inOrder.verify(request).addGauge(new DatadogGauge("timer.1MinuteRate", 3.0, timestamp, HOST, tags));
            inOrder.verify(request).send();

            verify(transport).prepare();
            verify(request).send();
            verifyNoMoreInteractions(transport, request);
        }
    }

    private record NameMetricFilter(String include) implements MetricFilter {
        public boolean matches(final String name, final Metric metric) {
            return (name.contains(include));
        }
    }

    private <T> SortedMap<String, T> map() {
        return new TreeMap<>();
    }

    private <T> SortedMap<String, T> map(String name, T metric) {
        var map = new TreeMap<String, T>();
        map.put(name, metric);
        return map;
    }

    private <T> Gauge<T> gauge(T value) {
        var gauge = mock(Gauge.class);
        when(gauge.getValue()).thenReturn(value);
        return gauge;
    }

    private void gaugeTestHelper(Number value, List<String> additionalTags) throws Exception {
        var inOrder = inOrder(transport, request);
        inOrder.verify(transport).prepare();
        inOrder.verify(request).addGauge(new DatadogGauge("gauge",
                value,
                timestamp,
                DatadogReporterTest.HOST,
                additionalTags));
        inOrder.verify(request).send();

        verify(transport).prepare();
        verify(request).send();
        verifyNoMoreInteractions(transport, request);
    }
}
