package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BenchmarkPushPop измеряет быстрый путь очереди: Push в items + WaitForNext
// из items (без горутин и таймеров).
func BenchmarkPushPop(b *testing.B) {
	q := NewQueue()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(&QueueMessage{Value: "x"})
		q.WaitForNext(0)
	}
}

// BenchmarkPushPopParallel измеряет тот же путь при конкурентном доступе —
// конкуренцию горутин за мьютекс очереди.
func BenchmarkPushPopParallel(b *testing.B) {
	q := NewQueue()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Push(&QueueMessage{Value: "x"})
			q.WaitForNext(0)
		}
	})
}

// BenchmarkPushToWaiter измеряет путь прямой передачи: Push отдаёт сообщение
// уже ожидающему получателю (ветка с waiters + пробуждение через канал).
func BenchmarkPushToWaiter(b *testing.B) {
	q := NewQueue()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg := q.WaitForNext(time.Hour)
			if msg != nil && msg.Value == "stop" {
				return
			}
		}
	}()
	time.Sleep(10 * time.Millisecond) // дать получателю зарегистрироваться

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(&QueueMessage{Value: "x"})
	}
	b.StopTimer()

	q.Push(&QueueMessage{Value: "stop"})
	<-done
}

// BenchmarkWaitForNextTimeout измеряет путь ожидания с таймаутом на пустой
// очереди: регистрация waiter, таймер, снятие с ожидания.
func BenchmarkWaitForNextTimeout(b *testing.B) {
	q := NewQueue()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if msg := q.WaitForNext(0); msg != nil {
			b.Fatalf("неожиданное сообщение: %q", msg.Value)
		}
	}
}

// BenchmarkHandlers измеряет HTTP-слой (обработчики) без сети:
// PUT одного сообщения и сразу GET его из очереди.
func BenchmarkHandlers(b *testing.B) {
	putRec := httptest.NewRecorder()
	getRec := httptest.NewRecorder()
	putReq := httptest.NewRequest(http.MethodPut, "/bench-h?v=x", nil)
	getReq := httptest.NewRequest(http.MethodGet, "/bench-h", nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		putHandler(putRec, putReq)

		getRec.Body.Reset()
		getHandler(getRec, getReq)
	}
}
