#!/usr/bin/env bash
#
# E2E-проверки сервиса очередей через curl.
#
# Использование:
#   scripts/curl_tests.sh                    # собрать и запустить сервис локально
#   scripts/curl_tests.sh http://host:port   # тестировать уже запущенный сервис
#
# Возвращает 0, если все проверки прошли, иначе 1.

set -uo pipefail

BASE_URL="${1:-}"
PORT="${PORT:-8080}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(dirname "$SCRIPT_DIR")"

WORK_DIR="$(mktemp -d)"
TMP="$WORK_DIR/tmp"
mkdir -p "$TMP"

FAILS=0
SRV=""

cleanup() {
	if [[ -n "$SRV" ]] && kill -0 "$SRV" 2>/dev/null; then
		kill "$SRV" 2>/dev/null
		wait "$SRV" 2>/dev/null || true
	fi
	rm -rf "$WORK_DIR"
}
trap cleanup EXIT

pass() { printf 'PASS  %s\n' "$1"; }
fail() {
	printf 'FAIL  %s\n' "$1"
	FAILS=$((FAILS + 1))
}

# check <описание> <ожидаемый_код> <ожидаемое_тело> <аргументы curl...>
check() {
	local desc="$1" want_code="$2" want_body="$3"
	shift 3
	local code body
	code=$(curl -sS -o "$TMP/body" -w '%{http_code}' "$@")
	body=$(cat "$TMP/body")
	if [[ "$code" == "$want_code" && "$body" == "$want_body" ]]; then
		pass "$desc (HTTP $code, body=\"$body\")"
	else
		fail "$desc: получено HTTP $code body=\"$body\", ожидалось HTTP $want_code body=\"$want_body\""
	fi
}

wait_ready() {
	local base="$1" i
	for i in $(seq 1 100); do
		if curl -s -o /dev/null "$base/__ready__"; then
			return 0
		fi
		sleep 0.1
	done
	return 1
}

if [[ -z "$BASE_URL" ]]; then
	BIN="$WORK_DIR/queue-service"
	echo "Собираю сервис..."
	if ! (cd "$MODULE_DIR" && go build -o "$BIN" ./app); then
		echo "Не удалось собрать сервис"
		exit 1
	fi
	"$BIN" "$PORT" >"$WORK_DIR/service.log" 2>&1 &
	SRV=$!
	BASE_URL="http://127.0.0.1:$PORT"
fi
BASE_URL="${BASE_URL%/}"

if ! wait_ready "$BASE_URL"; then
	echo "Сервис недоступен по $BASE_URL"
	[[ -n "$SRV" ]] && cat "$WORK_DIR/service.log"
	exit 1
fi

echo "Проверяю сервис: $BASE_URL"

# Уникальные имена очередей, чтобы не зависеть от состояния прошлых запусков.
SUF="$$-$RANDOM"
PET="pet-$SUF"
ROLE="role-$SUF"
WAITQ="wait-$SUF"
FIFOQ="fifo-$SUF"
LOADQ="load-$SUF"

echo "--- PUT / GET: базовый сценарий из README ---"
check "PUT /$PET?v=cat" 200 "" -X PUT "$BASE_URL/$PET?v=cat"
check "PUT /$PET?v=dog" 200 "" -X PUT "$BASE_URL/$PET?v=dog"
check "PUT /$ROLE?v=manager" 200 "" -X PUT "$BASE_URL/$ROLE?v=manager"
check "PUT /$ROLE?v=executive" 200 "" -X PUT "$BASE_URL/$ROLE?v=executive"

check "GET /$PET (1-й -> cat)" 200 "cat" "$BASE_URL/$PET"
check "GET /$PET (2-й -> dog)" 200 "dog" "$BASE_URL/$PET"
check "GET /$PET (пусто -> 404)" 404 "" "$BASE_URL/$PET"

check "GET /$ROLE (1-й -> manager)" 200 "manager" "$BASE_URL/$ROLE"
check "GET /$ROLE (2-й -> executive)" 200 "executive" "$BASE_URL/$ROLE"
check "GET /$ROLE (пусто -> 404)" 404 "" "$BASE_URL/$ROLE"

echo "--- URL-encoded значение ---"
check "PUT /$PET?v=hello%20world" 200 "" -X PUT "$BASE_URL/$PET?v=hello%20world"
check "GET декодирует значение" 200 "hello world" "$BASE_URL/$PET"

echo "--- Ошибки ---"
check "PUT без v (400)" 400 "" -X PUT "$BASE_URL/$PET"
check "GET ?timeout=1 на пустой (404)" 404 "" "$BASE_URL/$PET?timeout=1"

echo "--- Таймаут: GET ждёт поздний PUT ---"
(
	curl -s -o "$TMP/late.body" -w '%{http_code}' "$BASE_URL/$WAITQ?timeout=5" >"$TMP/late.code"
) &
LATE_PID=$!
sleep 0.5
check "PUT во время ожидания" 200 "" -X PUT "$BASE_URL/$WAITQ?v=late"
wait "$LATE_PID"
if [[ "$(cat "$TMP/late.code")" == "200" && "$(cat "$TMP/late.body")" == "late" ]]; then
	pass "GET дождался сообщения (HTTP 200, body=\"late\")"
else
	fail "GET с таймаутом: получено HTTP $(cat "$TMP/late.code") body=\"$(cat "$TMP/late.body")\""
fi

echo "--- Порядок получателей: кто первый запросил — тот первый получил ---"
(
	curl -s -o "$TMP/w1.body" "$BASE_URL/$FIFOQ?timeout=5"
) &
W1=$!
sleep 0.5
(
	curl -s -o "$TMP/w2.body" "$BASE_URL/$FIFOQ?timeout=5"
) &
W2=$!
sleep 0.5
check "PUT для 1-го получателя" 200 "" -X PUT "$BASE_URL/$FIFOQ?v=first"
sleep 0.1
check "PUT для 2-го получателя" 200 "" -X PUT "$BASE_URL/$FIFOQ?v=second"
wait "$W1" "$W2"
if [[ "$(cat "$TMP/w1.body")" == "first" && "$(cat "$TMP/w2.body")" == "second" ]]; then
	pass "получатели соблюли порядок (1-й -> first, 2-й -> second)"
else
	fail "порядок получателей нарушен: 1-й -> \"$(cat "$TMP/w1.body")\", 2-й -> \"$(cat "$TMP/w2.body")\""
fi

echo "--- Нет потерь/дубликатов при потоке сообщений ---"
N=50
i=0
while [[ $i -lt $N ]]; do
	i=$((i + 1))
	curl -s -o /dev/null -X PUT "$BASE_URL/$LOADQ?v=$i"
done

declare -A seen=()
dups=0
i=0
while [[ $i -lt $N ]]; do
	i=$((i + 1))
	v=$(curl -s "$BASE_URL/$LOADQ")
	if [[ -n "${seen[$v]:-}" ]]; then
		dups=$((dups + 1))
	fi
	seen[$v]=1
done
leftover=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/$LOADQ")
if [[ ${#seen[@]} -eq $N && $dups -eq 0 && "$leftover" == "404" ]]; then
	pass "получено $N уникальных сообщений, очередь пуста (HTTP 404)"
else
	fail "потеря/дубликаты: уникальных ${#seen[@]} из $N, дубликатов $dups, остаток HTTP $leftover"
fi

echo
if [[ $FAILS -eq 0 ]]; then
	echo "Все проверки прошли."
	exit 0
fi
echo "Провалено проверок: $FAILS"
exit 1
