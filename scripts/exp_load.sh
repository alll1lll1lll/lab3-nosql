#!/usr/bin/env bash
. "$(dirname "$0")/lib.sh"
OPS=${1:-20000}

table() {
    python -c '
import json, sys
rows = []
for path in sys.argv[1:]:
    d = json.load(open(path, encoding="utf-8"))
    for op in ("write", "read"):
        if op in d:
            s = d[op]
            rows.append((d["kind"], d["consistency"], d["concurrency"], op, d["throughputOpsPerSec"],
                         s["avgMs"], s["p95Ms"], s["p99Ms"], s["successRate"]))
print("%-6s %-12s %5s %-5s %10s %8s %8s %8s %6s" % ("kind", "CL", "conc", "op", "ops/s", "avg,ms", "p95,ms", "p99,ms", "ok%"))
for r in rows:
    print("%-6s %-12s %5d %-5s %10.0f %8.2f %8.2f %8.2f %6.1f" % r)
' "$@"
}

load() {
    local f="$RESULTS/load_${1}_c${2}_${3:-default}.json"
    api POST "/api/admin/cassandra/experiments/load?kind=$1&ops=$OPS&concurrency=$2${3:+&consistency=$3}" > "$f"
    echo "$f"
}

wait_up_nodes 3
banner "прогрев"
api POST "/api/admin/cassandra/experiments/load?kind=write&ops=5000&concurrency=32" > /dev/null

banner "основной профиль: LOCAL_QUORUM, 64 запроса в полёте"
FILES=("$(load write 64)" "$(load read 64)" "$(load mixed 64)")
table "${FILES[@]}"

banner "зависимость от параллелизма (запись)"
SWEEP=()
for c in 1 8 32 128 256; do SWEEP+=("$(load write $c)"); done
table "${SWEEP[@]}"

banner "уровень согласованности под нагрузкой (смешанная, 64)"
CLS=()
for cl in ONE LOCAL_QUORUM ALL; do CLS+=("$(load mixed 64 $cl)"); done
table "${CLS[@]}"

{
    echo "## основной профиль"; table "${FILES[@]}"
    echo; echo "## параллелизм"; table "${SWEEP[@]}"
    echo; echo "## уровни согласованности"; table "${CLS[@]}"
} > "$RESULTS/load_summary.txt"
echo "сводка: $RESULTS/load_summary.txt"
