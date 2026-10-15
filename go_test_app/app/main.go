package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Фактическое сообщение в очереди
type QueueMessage struct {
	Value string
}

// Ожидание получателя
type Waiter struct {
	Ch chan *QueueMessage
}

// Очередь сообщений + очередь ожидающих получателей
type Queue struct {
	mutex   sync.Mutex
	items   []*QueueMessage
	head    int
	waiters []*Waiter
}

func NewQueue() *Queue {
	return &Queue{
		items:   []*QueueMessage{},
		waiters: []*Waiter{},
	}
}

func (q *Queue) Push(msg *QueueMessage) {
	q.mutex.Lock()
	if len(q.waiters) > 0 {
		// Есть ожидающие — сразу отдаём первому
		waiter := q.waiters[0]
		q.waiters = q.waiters[1:]
		waiter.Ch <- msg
	} else {
		q.items = append(q.items, msg)
	}
	q.mutex.Unlock()
}

func (q *Queue) WaitForNext(timeout time.Duration) *QueueMessage {
	q.mutex.Lock()
	if q.head < len(q.items) {
		msg := q.items[q.head]
		q.items[q.head] = nil // не держим ссылку на отданное сообщение
		q.head++
		if q.head == len(q.items) {
			q.items = q.items[:0] // переиспользуем массив вместо роста с нуля
			q.head = 0
		}
		q.mutex.Unlock()
		return msg
	}
	// Без таймаута не ждём: очередь пуста — сразу отдаём 404,
	// не создавая waiter и таймер.
	if timeout <= 0 {
		q.mutex.Unlock()
		return nil
	}

	ch := make(chan *QueueMessage, 1)
	waiter := &Waiter{Ch: ch}
	q.waiters = append(q.waiters, waiter)
	q.mutex.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-ch:
		return msg
	case <-timer.C:
		q.mutex.Lock()
		// Убираем себя из waiters, если Push ещё не забрал нас.
		found := false
		for i, w := range q.waiters {
			if w == waiter {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				found = true
				break
			}
		}
		q.mutex.Unlock()

		if !found {
			// Push уже забрал этого waiter и положил сообщение в буфер ch.
			// Не теряем его — забираем из канала.
			return <-ch
		}
		waitTimeoutTotal.Add(1)
		return nil
	}
}

// Общий пул очередей по имени
var (
	qMutex sync.Mutex
	queues = map[string]*Queue{} // map[queueName] *Queue
)

// queryGet достаёт значение параметра из RawQuery, не строя map целиком,
// как это делает r.URL.Query() (по профилю — главный источник аллокаций).
// QueryUnescape не аллоцирует, если в строке нет % и +.
func queryGet(rawQuery, key string) string {
	for rawQuery != "" {
		var pair string
		if i := strings.IndexByte(rawQuery, '&'); i >= 0 {
			pair, rawQuery = rawQuery[:i], rawQuery[i+1:]
		} else {
			pair, rawQuery = rawQuery, ""
		}

		k, v, _ := strings.Cut(pair, "=")
		if k, err := url.QueryUnescape(k); err == nil && k == key {
			if v, err := url.QueryUnescape(v); err == nil {
				return v
			}
			return ""
		}
	}
	return ""
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	qname := strings.TrimPrefix(r.URL.Path, "/")
	if qname == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	v := queryGet(r.URL.RawQuery, "v")
	if v == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	qMutex.Lock()
	q, exists := queues[qname]
	if !exists {
		q = NewQueue()
		queues[qname] = q
	}
	qMutex.Unlock()

	q.Push(&QueueMessage{Value: v})
	putTotal.Add(1)

	w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	qname := strings.TrimPrefix(r.URL.Path, "/")
	if qname == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	timeout := 0.0
	timeoutStr := queryGet(r.URL.RawQuery, "timeout")
	if timeoutStr != "" {
		t, err := strconv.ParseFloat(timeoutStr, 64)
		if err != nil || t < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		timeout = t
	}

	qMutex.Lock()
	q, exists := queues[qname]
	if !exists {
		q = NewQueue()
		queues[qname] = q
	}
	qMutex.Unlock()

	var to time.Duration
	if timeout > 0 {
		to = time.Duration(timeout * float64(time.Second))
	}

	msg := q.WaitForNext(to)
	if msg == nil {
		getMissTotal.Add(1)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	getTotal.Add(1)

	_, _ = w.Write([]byte(msg.Value))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <port>")
		os.Exit(1)
	}
	port := os.Args[1]

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			metricsHandler(w, r)
			return
		}

		path := r.URL.Path
		if strings.HasPrefix(path, "/") && path != "/" && !strings.HasPrefix(path, "/favicon.ico") {
			qname := path[1:]
			if qname == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			switch r.Method {
			case http.MethodPut:
				putHandler(w, r)
			case http.MethodGet:
				getHandler(w, r)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	log.Printf("Server listening on port %s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
