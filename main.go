package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

func main() {

	otlpEndpoint, ok := os.LookupEnv("OTLP_ENDPOINT")
	if !ok {
		log.Fatal("Set up env var OTLP_ENDPOINT")
	}
	accessToken, ok := os.LookupEnv("ACCESS_TOKEN")
	if !ok {
		log.Fatal("Set up env var ACCESS_TOKEN")
	}
	prefix, ok := os.LookupEnv("PREFIX")
	if !ok {
		log.Fatal("Set up env var PREFIX")
	}

	if err := run(otlpEndpoint, accessToken, prefix); err != nil {
		log.Fatalf("error running otlp: %v", err)
	}
}

func run(otlpEndpoint string, accessToken string, prefix string) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	f := otlphttpexporter.NewFactory()
	c := f.CreateDefaultConfig()
	cfg := c.(*otlphttpexporter.Config)
	cfg.MetricsEndpoint = otlpEndpoint
	cfg.Headers = map[string]configopaque.String{
		"X-SF-Token": configopaque.String(accessToken),
	}
	exporter, err := f.CreateMetricsExporter(context.Background(), exporter.CreateSettings{
		ID: component.NewID("otlp"),
		TelemetrySettings: component.TelemetrySettings{
			Logger:                logger,
			TracerProvider:        tnoop.NewTracerProvider(),
			MeterProvider:         mnoop.NewMeterProvider(),
			MetricsLevel:          configtelemetry.LevelNone,
			Resource:              pcommon.NewResource(),
			ReportComponentStatus: nil,
		},
		BuildInfo: component.BuildInfo{},
	}, cfg)
	if err != nil {
		return err
	}

	keepRunning := &atomic.Bool{}
	keepRunning.Store(true)
	shutdownChan := make(chan os.Signal)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		<-shutdownChan
		keepRunning.Store(false)
	}()

	err = exporter.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		return err
	}
	defer func() {
		err := exporter.Shutdown(context.Background())
		if err != nil {
			logger.Error("error shutting down", zap.Error(err))
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	runExports(wg, prefix, exporter, keepRunning, logger)
	wg.Done()

	wg.Wait()

	return nil
}

func runExports(wg *sync.WaitGroup, prefix string, exporter exporter.Metrics, keepRunning *atomic.Bool, logger *zap.Logger) {
	var start sync.WaitGroup
	start.Add(1)
	// empty histogram
	go func() {
		start.Done()
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.empty", prefix))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending empty histogram", zap.Error(err))
			}
		}
		wg.Done()
	}()
	start.Wait()

	// simple gauge
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			g := metric.SetEmptyGauge()
			g.DataPoints().AppendEmpty().SetIntValue(1)
			metric.SetName(fmt.Sprintf("%s.gauge.check", prefix))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending gauge check", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// working histogram
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			dp := h.DataPoints().AppendEmpty()
			dp.SetMax(1)
			dp.SetMin(0)
			dp.SetSum(1)
			dp.SetCount(1)
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			metric.SetName(fmt.Sprintf("%s.histogram.complete", prefix))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending complete histogram", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with empty datapoint
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			h.DataPoints().AppendEmpty()
			metric.SetName(fmt.Sprintf("%s.histogram.dp.empty", prefix))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending empty histogram", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with no buckets
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.nobuckets", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(2.0)
			dp.SetMax(2.0)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with no buckets", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with min > max
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.minovermax", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(15.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with min > max", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with exemplars
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.exemplar", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(1.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with exemplar", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// delta histogram
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			metric.SetName(fmt.Sprintf("%s.histogram.delta", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(1.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with delta aggregation temporality", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// cumulative histogram
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			metric.SetName(fmt.Sprintf("%s.histogram.cumulative", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(1.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with cumulative aggregation temporality", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// no min histogram
	go func() {
		wg.Add(1)
		i := 0
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.nomin", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with no min", zap.Error(err))
			}
			i++
			if (i % 30) == 0 {
				logger.Error("Sent histograms", zap.Int("count", i))
			}
		}
		wg.Done()
	}()

	// no max histogram
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.nomax", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with no max", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// 33 buckets
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.33buckets", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(2.0)
			dp.SetMax(3.0)
			var buckets []uint64
			for i := 0; i < 33; i++ {
				buckets = append(buckets, uint64(i))
			}
			dp.BucketCounts().Append(buckets...)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with 33 buckets", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// 64 buckets
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.64buckets", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(2.0)
			dp.SetMax(3.0)
			var buckets []uint64
			for i := 0; i < 64; i++ {
				buckets = append(buckets, uint64(i))
			}
			dp.BucketCounts().Append(buckets...)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with 64 buckets", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with start timestamp older than end timestamp
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.startolderthanend", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-10 * time.Minute)))
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(1.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with start timestamp older than end timestamp", zap.Error(err))
			}
		}
		wg.Done()
	}()

	// histogram with attribute name longer than 255 characters
	go func() {
		wg.Add(1)
		for keepRunning.Load() {
			time.Sleep(1 * time.Second)
			metrics := pmetric.NewMetrics()
			metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			h := metric.SetEmptyHistogram()
			metric.SetName(fmt.Sprintf("%s.histogram.longattributename", prefix))
			dp := h.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("Lorem_ipsum_dolor_sit_amet,_consectetur_adipiscing_elit,_sed_do_eiusmod_tempor_incididunt_ut_labore_et_dolore_magna_aliqua._Ut_enim_ad_minim_veniam,_quis_nostrud_exercitation_ullamco_laboris_nisi_ut_aliquip_ex_ea_commodo_consequat._Duis_aute_irure_dolor_in_reprehenderit_in_voluptate_velit_esse_cillum_dolore_eu_fugiat_nulla_pariatur._Excepteur_sint_occaecat_cupidatat_non_proident,_sunt_in_culpa_qui_officia_deserunt_mollit_anim_id_est laborum.", "foo")
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-10 * time.Minute)))
			dp.SetSum(1)
			dp.SetCount(2)
			dp.SetMin(1.0)
			dp.SetMax(2.0)
			dp.BucketCounts().Append(1, 2, 3)
			dp.ExplicitBounds().Append(0.1, 0.2, 0.5)
			e := dp.Exemplars().AppendEmpty()
			e.SetTraceID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			e.SetSpanID([8]byte{0, 1, 2, 3, 4, 5, 6, 7})
			e.SetDoubleValue(42.0)
			e.SetIntValue(42)
			e.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			err := exporter.ConsumeMetrics(context.Background(), metrics)
			if err != nil {
				logger.Error("Error sending histogram with attribute key longer than 255 characters", zap.Error(err))
			}
		}
		wg.Done()
	}()
}
