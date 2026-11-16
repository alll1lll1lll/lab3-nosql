#!/usr/bin/env bash
. "$(dirname "$0")/lib.sh"
OPS=${1:-50}
LEVELS=ONE,QUORUM,ALL
TAG=qloss-$(date +%s)

run() {
    api POST "/api/admin/cassandra/experiments/consistency?levels=$LEVELS&ops=$OPS" > "$RESULTS/quorum_$1.json"
    summarize_levels < "$RESULTS/quorum_$1.json"
}

writer() { api GET /api/admin/cassandra/status | python -c "import sys,json;print('  eventWriter:', json.load(sys.stdin)['eventWriter'])"; }

create_org() {
    http_code POST /api/organizations "{\"id\":\"$TAG-$1\",\"name\":\"Quorum loss $1\",\"type\":\"STUDENT_CLUB\",\"contact\":\"lab3\"}"
}

q1() {
    local from
    from=$(python -c "import datetime as d;print((d.datetime.now().astimezone()-d.timedelta(hours=1)).isoformat(timespec='seconds'))")
    curl -sS -G -H "Person-Id: $ADMIN" "$BASE/api/events/by-type" --data-urlencode "type=ORGANIZATION_CREATED" \
        --data-urlencode "from=$from" --data-urlencode "size=200" ${1:+--data-urlencode "consistency=$1"} \
        -o "$RESULTS/quorum_q1_${2}_${1:-default}.json" -w "%{http_code}"
}

mine() { python -c "import sys,json;d=json.load(open(sys.argv[1],encoding='utf-8'));print(sum(1 for e in d.get('items',[]) if e['entityId'].startswith('$TAG')))" "$1"; }

hints() {
    dk exec cassandra-1 bash -c 'ls -l /opt/cassandra/data/hints/*.hints 2>/dev/null || echo "no hint files"' | sed 's/^/    /'
    echo "    host id узлов:"; nodetool status | awk '/^[UD]N/ {print "      " $2 "  " $7 "  " $8}'
}

cleanup() { dk start cassandra-2 cassandra-3 >/dev/null 2>&1 || true; }
trap cleanup EXIT

banner "A. 3 узла UP"
wait_up_nodes 3
run A_all_up

banner "B. остановлен cassandra-3"
dk stop cassandra-3 >/dev/null
wait_driver_view 2
run B_one_down

banner "C. остановлен ещё cassandra-2 -> кворум потерян"
dk stop cassandra-2 >/dev/null
wait_driver_view 1
run C_quorum_lost
echo "  -- то же одиночной вставкой (без LOGGED batch: batchlog требует ещё 2 живых узла с CL TWO)"
api POST "/api/admin/cassandra/experiments/consistency?levels=ONE,QUORUM&ops=$OPS&mode=single" > "$RESULTS/quorum_C_single_insert.json"
summarize_levels < "$RESULTS/quorum_C_single_insert.json"
echo "  CRUD: создание организаций в MongoDB -> HTTP $(create_org 1) $(create_org 2) $(create_org 3)"
sleep 12
writer
echo "  Q1 с LOCAL_QUORUM (по умолчанию) -> HTTP $(q1 '' C)"
echo "  Q1 с ONE                         -> HTTP $(q1 ONE C), наших событий: $(mine "$RESULTS/quorum_q1_C_ONE.json") из 3"
echo "  hints на cassandra-1 для лежащих узлов:"
hints

banner "D. узлы возвращаются"
dk start cassandra-2 cassandra-3 >/dev/null
wait_up_nodes 3
wait_driver_view 3
echo "  ждём повторную отправку очереди и доставку hints..."
sleep 15
writer
echo "  Q1 с QUORUM -> HTTP $(q1 QUORUM D), наших событий: $(mine "$RESULTS/quorum_q1_D_QUORUM.json") из 3"
echo "  hints после восстановления (доставлены и удалены):"
hints
run D_recovered
echo "результаты: $RESULTS/quorum_*.json"
