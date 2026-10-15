package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsHandler exposes the relayer's Prometheus metrics on a dedicated endpoint,
// separate from the payment API's so the two processes never contend for a port.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
