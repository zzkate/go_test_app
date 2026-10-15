package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Счётчики для /metrics. Только стандартная библиотека — без клиентов
// Prometheus, формат текстовый (exposition format).
var (
	putTotal         atomic.Int64 // успешные PUT
	getTotal         atomic.Int64 // GET, отдавшие сообщение
	getMissTotal     atomic.Int64 // GET без сообщения (404)
	waitTimeoutTotal atomic.Int64 // получатели, не дождавшиеся сообщения
)

// metricsHandler отдаёт метрики в текстовом формате Prometheus.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	writeCounter(w, "queue_put_total", "Успешные PUT-запросы.", putTotal.Load())
	writeCounter(w, "queue_get_total", "GET-запросы, отдавшие сообщение.", getTotal.Load())
	writeCounter(w, "queue_get_miss_total", "GET-запросы без сообщения (404).", getMissTotal.Load())
	writeCounter(w, "queue_wait_timeout_total", "Получатели, не дождавшиеся сообщения.", waitTimeoutTotal.Load())

	fmt.Fprintf(w, "# HELP queue_depth Сообщений в очереди.\n")
	fmt.Fprintf(w, "# TYPE queue_depth gauge\n")
	fmt.Fprintf(w, "# HELP queue_waiters Ожидающих получателей.\n")
	fmt.Fprintf(w, "# TYPE queue_waiters gauge\n")

	qMutex.Lock()
	for name, q := range queues {
		q.mutex.Lock()
		depth := len(q.items) - q.head
		waiters := len(q.waiters)
		q.mutex.Unlock()
		fmt.Fprintf(w, "queue_depth{queue=%q} %d\n", name, depth)
		fmt.Fprintf(w, "queue_waiters{queue=%q} %d\n", name, waiters)
	}
	qMutex.Unlock()
}

func writeCounter(w http.ResponseWriter, name, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	fmt.Fprintf(w, "%s %d\n", name, v)
}
