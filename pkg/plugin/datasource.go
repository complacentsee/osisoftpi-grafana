package plugin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/gorilla/websocket"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces- only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ backend.StreamHandler         = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	opts, err := settings.HTTPClientOptions()
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}
	cl, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("httpclient new: %w", err)
	}
	var webIDCache = map[string]WebIDCacheEntry{}
	var channelConstruct = map[string]StreamChannelConstruct{}

	scheduler := gocron.NewScheduler(time.UTC)
	scheduler.Every(5).Minute().Do(cleanWebIDCache, webIDCache)
	scheduler.StartAsync()

	return &Datasource{
		settings:                  settings,
		httpClient:                cl,
		webIDCache:                webIDCache,
		channelConstruct:          channelConstruct,
		scheduler:                 scheduler,
		websocketConnectionsMutex: &sync.Mutex{},
		sendersByWebIDMutex:       &sync.Mutex{},
		websocketConnections:      make(map[string]*websocket.Conn),
		sendersByWebID:            make(map[string]map[*backend.StreamSender]bool),
		streamChannels:            make(map[string]chan []byte),
	}, nil
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	d.httpClient.CloseIdleConnections()
}

// Main entry point for a query. Called by Grafana when a query is executed.
// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).

// TODO: Add support for regex replace of frame names
// TODO: Missing functionality: Fix summaries
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	processedQueries := make(map[string][]PiProcessedQuery)
	datasourceUID := req.PluginContext.DataSourceInstanceSettings.UID

	// Process queries and turn them into a suitable format for the PI Web API
	for _, q := range req.Queries {
		processedQueries[q.RefID] = d.processQuery(ctx, q, datasourceUID)
	}

	// Send the queries to the PI Web API
	processedQueries_temp := d.batchRequest(ctx, processedQueries)

	// Convert the PI Web API response into Grafana frames
	response := d.processBatchtoFrames(processedQueries_temp)

	return response, nil
}

func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	var isAllowed = true
	var allowedBasePaths = []string{
		"/assetdatabases",
		"/elements",
		"/assetservers",
		"/points",
		"/attributes",
		"/dataservers",
		"/annotations",
	}
	for _, path := range allowedBasePaths {
		if strings.HasPrefix(req.Path, path) {
			isAllowed = true
			break
		}
	}

	if !isAllowed {
		return sender.Send(&backend.CallResourceResponse{
			Status: http.StatusForbidden,
			Body:   nil,
		})
	}

	r, err := d.apiGet(ctx, req.URL)
	if err != nil {
		return err
	}
	return sender.Send(&backend.CallResourceResponse{
		Status: http.StatusOK,
		Body:   r,
	})
}

// CheckHealth performs a request to the specified data source and returns an error if the HTTP handler did not return
// a 200 OK response.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, d.settings.URL, nil)
	if err != nil {
		return newHealthCheckErrorf("could not create request"), nil
	}
	if d.settings.BasicAuthEnabled {
		r.SetBasicAuth(d.settings.BasicAuthUser, d.settings.DecryptedSecureJSONData["basicAuthPassword"])
	}
	resp, err := d.httpClient.Do(r)
	if err != nil {
		return newHealthCheckErrorf("request error"), nil
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("check health: failed to close response body", "err", err.Error())
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return newHealthCheckErrorf("got response code %d", resp.StatusCode), nil
	}
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

// newHealthCheckErrorf returns a new *backend.CheckHealthResult with its status set to backend.HealthStatusError
// and the specified message, which is formatted with Sprintf.
func newHealthCheckErrorf(format string, args ...interface{}) *backend.CheckHealthResult {
	return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: fmt.Sprintf(format, args...)}
}
