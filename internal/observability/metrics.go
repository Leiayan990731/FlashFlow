package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry       *prometheus.Registry
	HTTPRequests   *prometheus.CounterVec
	HTTPDuration   *prometheus.HistogramVec
	HTTPInFlight   prometheus.Gauge
	Reservations   *prometheus.CounterVec
	StreamMessages *prometheus.CounterVec
	KafkaMessages  *prometheus.CounterVec
	Orders         *prometheus.CounterVec
	DependencyUp   *prometheus.GaugeVec
}

func New(service string) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		Registry: registry,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "flashflow", Subsystem: service, Name: "http_requests_total",
			Help: "Total HTTP requests by route, method, and status.",
		}, []string{"route", "method", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "flashflow", Subsystem: service, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency.",
			Buckets: []float64{0.001, 0.003, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"route", "method"}),
		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "flashflow", Subsystem: service, Name: "http_in_flight",
			Help: "Current in-flight HTTP requests.",
		}),
		Reservations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "flashflow", Subsystem: service, Name: "reservations_total",
			Help: "Inventory reservation outcomes.",
		}, []string{"result"}),
		StreamMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "flashflow", Subsystem: service, Name: "stream_messages_total",
			Help: "Redis Stream relay outcomes.",
		}, []string{"result"}),
		KafkaMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "flashflow", Subsystem: service, Name: "kafka_messages_total",
			Help: "Kafka message outcomes.",
		}, []string{"result"}),
		Orders: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "flashflow", Subsystem: service, Name: "orders_total",
			Help: "Order processing outcomes.",
		}, []string{"result"}),
		DependencyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "flashflow", Subsystem: service, Name: "dependency_up",
			Help: "Whether a required dependency is reachable.",
		}, []string{"dependency"}),
	}
	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.HTTPInFlight, m.Reservations,
		m.StreamMessages, m.KafkaMessages, m.Orders, m.DependencyUp,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
