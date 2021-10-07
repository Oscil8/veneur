package prometheus

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/stripe/veneur/v14/samplers"
	"github.com/stripe/veneur/v14/sinks"
	"github.com/stripe/veneur/v14/sinks/prometheus/mapper"
	"github.com/stripe/veneur/v14/sinks/prometheus/prompb"
	"github.com/stripe/veneur/v14/ssf"
	"github.com/stripe/veneur/v14/trace"

	"github.com/prometheus/common/config"
	"github.com/sirupsen/logrus"
)

// RemoteWriteExporter is the metric sink implementation for Prometheus via remote write.
type RemoteWriteExporter struct {
	addr        string
	headers     []string
	logger      *logrus.Logger
	traceClient *trace.Client
	promClient  *http.Client
	flushMaxPerBody,
	flushMaxConcurrency int
}

// NewRemoteWriteExporter returns a new RemoteWriteExporter, validating params.
func NewRemoteWriteExporter(addr string, bearerToken string, flushMaxPerBody int, flushMaxConcurrency int, logger *logrus.Logger) (*RemoteWriteExporter, error) {
	if _, err := url.ParseRequestURI(addr); err != nil {
		return nil, err
	}

	httpClientConfig := config.HTTPClientConfig{BearerToken: config.Secret(bearerToken)}
	httpClient, err := config.NewClientFromConfig(httpClientConfig, "venuerSink", false)
	if err != nil {
		return nil, err
	}

	// some defaults
	if flushMaxPerBody <= 0 {
		flushMaxPerBody = 5000
	}
	if flushMaxConcurrency <= 0 {
		flushMaxConcurrency = 10
	}

	return &RemoteWriteExporter{
		addr:                addr,
		logger:              logger,
		promClient:          httpClient,
		flushMaxPerBody:     flushMaxPerBody,
		flushMaxConcurrency: flushMaxConcurrency,
	}, nil
}

// Name returns the name of this sink.
func (prw *RemoteWriteExporter) Name() string {
	return "prometheus_rw"
}

// Start begins the sink.
func (prw *RemoteWriteExporter) Start(cl *trace.Client) error {
	prw.traceClient = cl
	return nil
}

// Flush sends metrics to the Statsd Exporter in batches.
func (prw *RemoteWriteExporter) Flush(ctx context.Context, interMetrics []samplers.InterMetric) error {
	span, _ := trace.StartSpanFromContext(ctx, "")
	defer span.ClientFinish(prw.traceClient)

	prommetrics := prw.finalizeMetrics(interMetrics)

	// break the metrics into chunks of approximately equal size, such that
	// each chunk is less than the limit
	// we compute the chunks using rounding-up integer division
	workers := ((len(prommetrics) - 1) / prw.flushMaxPerBody) + 1
	chunkSize := ((len(prommetrics) - 1) / workers) + 1
	prw.logger.WithField("workers", workers).Debug("Worker count chosen")
	prw.logger.WithField("chunkSize", chunkSize).Debug("Chunk size chosen")
	var wg sync.WaitGroup
	flushStart := time.Now()

	// a blocking channel to keep concurrency under control
	semaphoreChan := make(chan struct{}, prw.flushMaxConcurrency)
	defer close(semaphoreChan)

	for i := 0; i < workers; i++ {
		chunk := prommetrics[i*chunkSize:]
		if i < workers-1 {
			// trim to chunk size unless this is the last one
			chunk = chunk[:chunkSize]
		}
		wg.Add(1)

		if prw.flushMaxConcurrency > 0 {
			// block until the semaphore channel has room
			semaphoreChan <- struct{}{}
		}

		go func() {
			defer func() {
				if prw.flushMaxConcurrency > 0 {
					// clear a spot in the semaphore channel
					<-semaphoreChan
				}
			}()
			prw.flushPart(span.Attach(ctx), chunk, &wg)
		}()
	}
	wg.Wait()
	tags := map[string]string{"sink": prw.Name()}
	span.Add(
		ssf.Timing(sinks.MetricKeyMetricFlushDuration, time.Since(flushStart), time.Nanosecond, tags),
		ssf.Count(sinks.MetricKeyTotalMetricsFlushed, float32(len(prommetrics)), tags),
	)
	prw.logger.WithField("metrics", len(prommetrics)).Info("Completed flush to Prometheus Remote Write")
	return nil
}

// FlushOtherSamples sends events to SignalFx. This is a no-op for Prometheus
// sinks as Prometheus does not support other samples.
func (prw *RemoteWriteExporter) FlushOtherSamples(ctx context.Context, samples []ssf.SSFSample) {
}

func (prw *RemoteWriteExporter) finalizeMetrics(metrics []samplers.InterMetric) []prompb.TimeSeries {
	promMetrics := make([]prompb.TimeSeries, 0, len(metrics))

	for _, m := range metrics {
		if !sinks.IsAcceptableMetric(m, prw) {
			continue
		}

		seenKeys := make(map[string]struct{}, len(m.Tags)+1)
		SEEN := struct{}{} // sentinel value for set

		promLabels := make([]prompb.Label, 0, len(m.Tags)+1)
		promLabels = append(promLabels, prompb.Label{Name: "__name__", Value: mapper.EscapeMetricName(m.Name)})
		seenKeys["__name__"] = SEEN

		for _, tag := range m.Tags {
			var key, value string
			if strings.Contains(tag, ":") {
				keyvalpair := strings.SplitN(tag, ":", 2)
				key, value = mapper.EscapeMetricName(keyvalpair[0]), keyvalpair[1]
			} else {
				key, value = mapper.EscapeMetricName(tag), "true"
			}

			if _, ok := seenKeys[key]; ok {
				prw.logger.Warnf("Dropping label %s: %s for metric %s; duplicate key", key, value, m.Name)
				continue
			}
			seenKeys[key] = SEEN
			promLabels = append(promLabels, prompb.Label{Name: key, Value: value})
		}

		promMetrics = append(promMetrics, prompb.TimeSeries{
			Labels:  promLabels,
			Samples: []prompb.Sample{{Timestamp: m.Timestamp * time.Second.Milliseconds(), Value: m.Value}},
		})
	}

	return promMetrics
}

func (prw *RemoteWriteExporter) flushPart(ctx context.Context, tsSlice []prompb.TimeSeries, wg *sync.WaitGroup) {
	defer wg.Done()

	req, err := prw.buildRequest(tsSlice)
	if err != nil {
		return // already logged failure
	}

	retries := 5
	backoff := 50 * time.Millisecond
	for {
		_, _, err = prw.store(ctx, req)
		if err != nil {
			_, recoverable := err.(recoverableError)
			if recoverable {
				retries--
				if retries < 0 {
					prw.logger.Errorf("Failed: %v, aborting retries", err.Error())
					return
				}

				prw.logger.Warnf("Failed: %v, retrying after %d ms (%d tries left)", err.Error(), backoff.Milliseconds(), retries)
				time.Sleep(backoff)
				backoff *= 2
				continue
			}

			// not recoverable
			prw.logger.Errorf("Failed: %v, not retryable", err.Error())
		}
		return
	}
}

func (prw *RemoteWriteExporter) buildRequest(tsSlice []prompb.TimeSeries) (req []byte, err error) {
	request := prompb.WriteRequest{Timeseries: tsSlice}

	var reqBuf []byte
	if reqBuf, err = proto.Marshal(&request); err != nil {
		prw.logger.Errorf("failed to marshal the WriteRequest %v", err)
		return nil, err
	}

	compressed := snappy.Encode(nil, reqBuf)
	if err != nil {
		prw.logger.Errorf("failed to compress the WriteRequest %v", err)
		return nil, err
	}
	return compressed, nil
}

// used to signify that the error from store is worth retry-ing
type recoverableError struct {
	error
}

// storeRequest sends a marshalled batch of samples to the HTTP endpoint
// returns statuscode or -1 if the request didn't get to the server
// response body is returned as []byte
func (prw *RemoteWriteExporter) store(ctx context.Context, req []byte) (int, []byte, error) {
	httpReq, err := http.NewRequest("POST", prw.addr, bytes.NewReader(req))
	if err != nil {
		return -1, nil, err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", fmt.Sprintf("Venuer Prometheus RW sink"))

	ctx, cancel := context.WithTimeout(ctx, 9*time.Second)
	defer cancel()

	httpResp, err := prw.promClient.Do(httpReq.WithContext(ctx))
	if err != nil {
		return -1, nil, recoverableError{err}
	}
	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 2048 /*maxErrMsgLen*/))
	var responseBody []byte
	if scanner.Scan() {
		responseBody = scanner.Bytes()
	}

	if httpResp.StatusCode/100 != 2 {
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, string(responseBody))
	}
	if httpResp.StatusCode/100 == 5 {
		return httpResp.StatusCode, responseBody, recoverableError{err}
	}
	return httpResp.StatusCode, responseBody, err
}
