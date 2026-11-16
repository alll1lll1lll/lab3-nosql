set -euo pipefail

BASE=${BASE:-http://localhost:8080}
ADMIN=${ADMIN:-100000}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
RESULTS=${RESULTS:-$SCRIPT_DIR/../results}
mkdir -p "$RESULTS"
export PYTHONIOENCODING=utf-8

dk() {
    if command -v docker >/dev/null 2>&1; then
        docker "$@"
    else
        MSYS_NO_PATHCONV=1 wsl -d "${WSL_DISTRO:-Ubuntu}" -- bash -lc "$(printf '%q ' docker "$@")"
    fi
}

nodetool() { dk exec cassandra-1 nodetool "$@"; }

api() {
    curl -sS -X "$1" -H "Person-Id: $ADMIN" -H "Content-Type: application/json" ${3:+-d "$3"} "$BASE$2"
}

http_code() {
    curl -sS -o /dev/null -w "%{http_code}" -X "$1" -H "Person-Id: $ADMIN" -H "Content-Type: application/json" \
        ${3:+-d "$3"} "$BASE$2"
}

banner() { printf '\n========== %s ==========\n' "$*"; }

up_nodes() {
    dk exec "${1:-cassandra-1}" nodetool status 2>/dev/null | grep -c '^UN' || true
}

wait_up_nodes() {
    local want=$1 node=${2:-cassandra-1}
    for _ in $(seq 90); do
        [ "$(up_nodes "$node")" -ge "$want" ] && return 0
        sleep 2
    done
    echo "не дождались $want узлов UN" >&2
    return 1
}

wait_driver_view() {
    local want_up=$1
    for _ in $(seq 60); do
        local n
        n=$(api GET /api/admin/cassandra/status | python -c "import sys,json;print(sum(1 for n in json.load(sys.stdin)['nodes'] if n['state']=='UP'))" 2>/dev/null || echo -1)
        [ "$n" = "$want_up" ] && return 0
        sleep 2
    done
    echo "драйвер не увидел $want_up узлов UP" >&2
}

summarize_levels() {
    python -c '
import json, sys
d = json.load(sys.stdin)
if "levels" not in d:
    print(json.dumps(d, ensure_ascii=False)); sys.exit()
print("  узлы:", ", ".join(d.get("upNodes", [])))
print("  %-7s %-5s %8s %8s %8s %8s  %s" % ("CL", "op", "ok%", "avg,ms", "p95,ms", "max,ms", "ошибки"))
for cl, v in d["levels"].items():
    for op in ("write", "read"):
        s = v[op]
        print("  %-7s %-5s %8.1f %8.2f %8.2f %8.2f  %s" % (cl, op, s["successRate"], s["avgMs"], s["p95Ms"],
              s["maxMs"], s.get("errors", "")))
'
}
