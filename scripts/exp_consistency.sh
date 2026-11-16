#!/usr/bin/env bash
. "$(dirname "$0")/lib.sh"
OPS=${1:-300}
LEVELS=ONE,ALL

run() {
    local stage=$1
    api POST "/api/admin/cassandra/experiments/consistency?levels=$LEVELS&ops=$OPS" > "$RESULTS/consistency_$stage.json"
    summarize_levels < "$RESULTS/consistency_$stage.json"
}

CPUS=$(dk info --format '{{.NCPU}}')
cleanup() {
    dk unpause cassandra-3 >/dev/null 2>&1 || true
    dk update --cpus "$CPUS" cassandra-3 >/dev/null 2>&1 || true
    dk start cassandra-3 >/dev/null 2>&1 || true
}
trap cleanup EXIT

banner "A. все узлы UP"
wait_up_nodes 3
run A_healthy

banner "B. cassandra-3 ограничен 0.3 CPU"
dk update --cpus 0.3 cassandra-3 >/dev/null
run B_slow_replica
dk update --cpus "$CPUS" cassandra-3 >/dev/null

banner "C. cassandra-3 заморожен (docker pause)"
dk pause cassandra-3 >/dev/null
run C_paused_replica
dk unpause cassandra-3 >/dev/null
wait_up_nodes 3
wait_driver_view 3

banner "D. cassandra-3 остановлен"
dk stop cassandra-3 >/dev/null
wait_driver_view 2
run D_node_down

banner "восстановление"
dk start cassandra-3 >/dev/null
wait_up_nodes 3
wait_driver_view 3
echo "результаты: $RESULTS/consistency_*.json"
