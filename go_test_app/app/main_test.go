package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// waitForWaiters ждёт, пока в очереди зарегистрируется n получателей.
func waitForWaiters(t *testing.T, q *Queue, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		q.mutex.Lock()
		got := len(q.waiters)
		q.mutex.Unlock()
		if got >= n {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
	t.Fatalf("не дождались %d ожидающих получателей", n)
}

func waitValue(q *Queue, timeout time.Duration) string {
	if msg := q.WaitForNext(timeout); msg != nil {
		return msg.Value
	}
	return ""
}

func TestPushPopFIFO(t *testing.T) {
	q := NewQueue()
	for _, v := range []string{"a", "b", "c"} {
		q.Push(&QueueMessage{Value: v})
	}

	for _, want := range []string{"a", "b", "c"} {
		msg := q.WaitForNext(0)
		if msg == nil || msg.Value != want {
			t.Fatalf("получено %v; ожидалось %q", msg, want)
		}
	}

	if msg := q.WaitForNext(0); msg != nil {
		t.Fatalf("очередь должна быть пуста, получено %q", msg.Value)
	}
}

func TestWaitTimeoutReturnsNil(t *testing.T) {
	q := NewQueue()
	start := time.Now()
	if msg := q.WaitForNext(50 * time.Millisecond); msg != nil {
		t.Fatalf("ожидали nil, получено %q", msg.Value)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("вернулись раньше таймаута: %v", elapsed)
	}
}

func TestPushWakesWaiter(t *testing.T) {
	q := NewQueue()

	result := make(chan string, 1)
	go func() { result <- waitValue(q, time.Second) }()

	waitForWaiters(t, q, 1)
	q.Push(&QueueMessage{Value: "hello"})

	select {
	case got := <-result:
		if got != "hello" {
			t.Fatalf("получено %q; ожидалось \"hello\"", got)
		}
	case <-time.After(time.Second):
		t.Fatal("получатель не был разбужен")
	}
}

// TestWaitersFIFO проверяет, что при нескольких ожидающих первое сообщение
// достаётся тому, кто раньше запросил.
func TestWaitersFIFO(t *testing.T) {
	q := NewQueue()

	first := make(chan string, 1)
	second := make(chan string, 1)

	go func() { first <- waitValue(q, time.Second) }()
	waitForWaiters(t, q, 1)
	go func() { second <- waitValue(q, time.Second) }()
	waitForWaiters(t, q, 2)

	q.Push(&QueueMessage{Value: "one"})
	if got := <-first; got != "one" {
		t.Fatalf("первый получатель получил %q; ожидалось \"one\"", got)
	}

	q.Push(&QueueMessage{Value: "two"})
	if got := <-second; got != "two" {
		t.Fatalf("второй получатель получил %q; ожидалось \"two\"", got)
	}
}

// TestTimeoutDeliveryRaceNoLoss — ключевой тест: GET без таймаута
// (или с крошечным таймаутом) и одновременный PUT. Сообщение не должно
// исчезнуть: оно либо доставлено получателю, либо осталось в очереди.
func TestTimeoutDeliveryRaceNoLoss(t *testing.T) {
	for i := 0; i < 50000; i++ {
		q := NewQueue()

		got := make(chan *QueueMessage, 1)
		go func() { got <- q.WaitForNext(0) }()

		q.Push(&QueueMessage{Value: "x"})

		msg := <-got
		if msg != nil {
			if msg.Value != "x" {
				t.Fatalf("итерация %d: получено %q", i, msg.Value)
			}
			continue
		}

		// Получатель вернул nil — сообщение обязано остаться в очереди.
		if left := q.WaitForNext(0); left == nil {
			t.Fatalf("итерация %d: сообщение потеряно", i)
		}
	}
}

// TestNoMessageLossUnderLoad — нагрузочный тест: сколько положили, столько
// и должны забрать (без дублей и пропаж).
func TestNoMessageLossUnderLoad(t *testing.T) {
	q := NewQueue()
	const total = 3000

	received := make(chan string, total)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.After(20 * time.Second)
		for n := 0; n < total; n++ {
			for {
				select {
				case <-deadline:
					return
				default:
				}
				if msg := q.WaitForNext(time.Millisecond); msg != nil {
					received <- msg.Value
					break
				}
			}
		}
	}()

	for i := 0; i < total; i++ {
		q.Push(&QueueMessage{Value: strconv.Itoa(i)})
		if i%250 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	wg.Wait()
	close(received)

	seen := make(map[string]bool, total)
	for v := range received {
		if seen[v] {
			t.Errorf("дубликат сообщения %q", v)
		}
		seen[v] = true
	}
	if len(seen) != total {
		t.Errorf("получено %d сообщений из %d — часть потеряна", len(seen), total)
	}
}

func TestPutHandlerOKAndBadRequest(t *testing.T) {
	rec := httptest.NewRecorder()
	putHandler(rec, httptest.NewRequest(http.MethodPut, "/httptest?v=cat", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT: статус %d; ожидался 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	putHandler(rec, httptest.NewRequest(http.MethodPut, "/httptest", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT без v: статус %d; ожидался 400", rec.Code)
	}
}

func TestGetHandlerFIFOAndNotFound(t *testing.T) {
	putHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/gettest?v=cat", nil))
	putHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/gettest?v=dog", nil))

	for _, want := range []string{"cat", "dog"} {
		rec := httptest.NewRecorder()
		getHandler(rec, httptest.NewRequest(http.MethodGet, "/gettest", nil))
		if rec.Code != http.StatusOK || rec.Body.String() != want {
			t.Fatalf("GET: статус %d, тело %q; ожидалось 200 %q", rec.Code, rec.Body.String(), want)
		}
	}

	rec := httptest.NewRecorder()
	getHandler(rec, httptest.NewRequest(http.MethodGet, "/gettest", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET пустой очереди: статус %d; ожидался 404", rec.Code)
	}
}

func TestGetHandlerTimeoutReceives(t *testing.T) {
	go func() {
		time.Sleep(50 * time.Millisecond)
		putHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/slowq?v=late", nil))
	}()

	rec := httptest.NewRecorder()
	getHandler(rec, httptest.NewRequest(http.MethodGet, "/slowq?timeout=2", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "late" {
		t.Fatalf("GET с таймаутом: статус %d, тело %q; ожидалось 200 \"late\"", rec.Code, rec.Body.String())
	}
}
