package main

import (
	"fmt"
	"log"
	"net/http"
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
	defer q.mutex.Unlock()

	if len(q.waiters) > 0 {
		// Есть ожидающие — сразу отдаём первому
		waiter := q.waiters[0]
		q.waiters = q.waiters[1:]
		waiter.Ch <- msg
	} else {
		q.items = append(q.items, msg)
	}
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
		return nil
	}
}

// Общий пул очередей по имени
var (
	qMutex sync.Mutex
	queues = map[string]*Queue{} // map[queueName] *Queue
)

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

	v := r.URL.Query().Get("v")
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
	timeoutStr := r.URL.Query().Get("timeout")
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
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, _ = w.Write([]byte(msg.Value))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <port>")
		os.Exit(1)
	}
	port := os.Args[1]

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
