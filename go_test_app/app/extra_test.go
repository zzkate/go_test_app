package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestQueryGet(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		key  string
		want string
	}{
		{"один параметр", "v=cat", "v", "cat"},
		{"не первый", "a=1&v=cat", "v", "cat"},
		{"не последний", "v=cat&a=1", "v", "cat"},
		{"первый побеждает", "v=cat&v=dog", "v", "cat"},
		{"нет ключа", "a=1", "v", ""},
		{"пустой raw", "", "v", ""},
		{"пустое значение", "v=", "v", ""},
		{"без знака =", "v", "v", ""},
		{"URL-encoded значение", "v=hello%20world", "v", "hello world"},
		{"плюс как пробел", "v=hello+world", "v", "hello world"},
		{"= в значении", "v=a=b", "v", "a=b"},
		{"битый escape", "v=%zz", "v", ""},
		{"хвостовой &", "a=1&", "a", "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queryGet(tc.raw, tc.key); got != tc.want {
				t.Fatalf("queryGet(%q, %q) = %q; ожидалось %q", tc.raw, tc.key, got, tc.want)
			}
		})
	}
}

// FuzzQueryGet сверяет наш парсер с эталонным url.ParseQuery.
func FuzzQueryGet(f *testing.F) {
	for _, seed := range []string{"v=cat", "a=1&v=dog", "", "v=", "v=hello%20world", "v=a=b", "timeout=abc", "v=a&v=b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		values, err := url.ParseQuery(raw)
		if err != nil {
			return // для невалидного запроса поведение не определено
		}
		for key := range values {
			if got, want := queryGet(raw, key), values.Get(key); got != want {
				t.Fatalf("queryGet(%q, %q) = %q; ParseQuery = %q", raw, key, got, want)
			}
		}
		if got := queryGet(raw, "\x00"); got != "" {
			t.Fatalf("queryGet(%q, несуществующий) = %q; ожидалось \"\"", raw, got)
		}
	})
}

// TestWaitForNextZeroAllocs фиксирует оптимизацию: без таймаута ожидание
// на пустой очереди не аллоцирует (не создаёт waiter и таймер).
func TestWaitForNextZeroAllocs(t *testing.T) {
	q := NewQueue()
	allocs := testing.AllocsPerRun(1000, func() {
		q.WaitForNext(0)
	})
	if allocs != 0 {
		t.Fatalf("WaitForNext(0) аллоцирует %.2f раз/вызов; ожидалось 0", allocs)
	}
}

func TestHandlerEdgeCases(t *testing.T) {
	cases := []struct {
		name   string
		h      http.HandlerFunc
		method string
		target string
		want   int
	}{
		{"PUT не PUT -> 405", putHandler, http.MethodPost, "/e?v=x", http.StatusMethodNotAllowed},
		{"GET не GET -> 405", getHandler, http.MethodPut, "/e", http.StatusMethodNotAllowed},
		{"PUT без v -> 400", putHandler, http.MethodPut, "/e", http.StatusBadRequest},
		{"PUT с пустым v -> 400", putHandler, http.MethodPut, "/e?v=", http.StatusBadRequest},
		{"PUT без имени очереди -> 400", putHandler, http.MethodPut, "/?v=x", http.StatusBadRequest},
		{"GET timeout не число -> 400", getHandler, http.MethodGet, "/e?timeout=abc", http.StatusBadRequest},
		{"GET timeout < 0 -> 400", getHandler, http.MethodGet, "/e?timeout=-1", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.h(rec, httptest.NewRequest(tc.method, tc.target, nil))
			if rec.Code != tc.want {
				t.Fatalf("статус %d; ожидался %d", rec.Code, tc.want)
			}
		})
	}
}

func TestQueryParamsInHandlers(t *testing.T) {
	const qname = "params-test"

	rec := httptest.NewRecorder()
	putHandler(rec, httptest.NewRequest(http.MethodPut, "/"+qname+"?a=1&v=hello%20world", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT: статус %d; ожидался 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	getHandler(rec, httptest.NewRequest(http.MethodGet, "/"+qname+"?b=2&timeout=0", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "hello world" {
		t.Fatalf("GET: статус %d, тело %q; ожидалось 200 \"hello world\"", rec.Code, rec.Body.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	putHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/metrics-test?v=1", nil))

	rec := httptest.NewRecorder()
	metricsHandler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("статус %d; ожидался 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q; ожидался text/plain", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"queue_put_total",
		"queue_get_total",
		"queue_get_miss_total",
		"queue_wait_timeout_total",
		`queue_depth{queue="metrics-test"}`,
		`queue_waiters{queue="metrics-test"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("в /metrics нет %q\n--- вывод ---\n%s", want, body)
		}
	}
}
