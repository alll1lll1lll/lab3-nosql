#!/usr/bin/env bash
. "$(dirname "$0")/lib.sh"
COUNT=${1:-300000}
KS=booking_events
LAB=hot_partition_lab
OUT=$RESULTS/hot_partition

banner "сброс данных прошлого запуска"
api POST /api/admin/cassandra/experiments/hot-partition/reset; echo

banner "запись $COUNT событий + сэмплирование горячих партиций (toppartitions, 30 с)"
nodetool toppartitions -s 20 -k 5 -a WRITES $KS events_by_type_unbucketed 30000 > "$OUT"_top_unbucketed.txt 2>&1 &
TOP1=$!
nodetool toppartitions -s 20 -k 5 -a WRITES $KS events_by_type 30000 > "$OUT"_top_bucketed.txt 2>&1 &
TOP2=$!
sleep 2
api POST "/api/admin/cassandra/experiments/hot-partition/fill?count=$COUNT&concurrency=128" > "$OUT"_fill.json
python -m json.tool "$OUT"_fill.json
wait $TOP1 $TOP2 || true
echo "--- toppartitions, без бакета:"; sed -n '1,12p' "$OUT"_top_unbucketed.txt
echo "--- toppartitions, с бакетом:";  sed -n '1,12p' "$OUT"_top_bucketed.txt

banner "flush memtable -> sstable, чтобы появились гистограммы размеров партиций"
for n in cassandra-1 cassandra-2 cassandra-3; do dk exec $n nodetool flush $KS; dk exec $n nodetool flush $LAB; done

banner "размеры партиций (nodetool tablehistograms, cassandra-1)"
{
    echo "### $KS.events_by_type (с бакетом)";  nodetool tablehistograms $KS events_by_type
    echo "### $KS.events_by_type_unbucketed";   nodetool tablehistograms $KS events_by_type_unbucketed
} | tee "$OUT"_histograms.txt

banner "максимальная партиция (nodetool tablestats)"
nodetool tablestats $KS.events_by_type $KS.events_by_type_unbucketed \
    | grep -E "Table:|Number of partitions|Compacted partition (minimum|maximum|mean) bytes|Space used \(live\)" \
    | tee "$OUT"_tablestats.txt

banner "RF=1: на каком узле лежат данные (write count / partitions по узлам)"
for n in cassandra-1 cassandra-2 cassandra-3; do
    echo "--- $n"
    dk exec $n nodetool tablestats $LAB.events_by_type $LAB.events_by_type_unbucketed \
        | grep -E "Table:|Local write count|Number of partitions|Space used \(live\)|Compacted partition maximum bytes"
done | tee "$OUT"_rf1_nodes.txt

banner "предупреждения Cassandra о крупных партициях (guardrail partition_size_warn_threshold = 10MiB)"
for n in cassandra-1 cassandra-2 cassandra-3; do
    dk logs --since 30m $n 2>&1 | grep -iE "large partition|partition_size|Guardrail" | tail -3 | sed "s/^/[$n] /"
done | tee "$OUT"_warnings.txt

banner "отчёт приложения: строки по партициям, реплики, задержка чтения"
api GET "/api/admin/cassandra/experiments/hot-partition/report?repeats=30" > "$OUT"_report.json
python -m json.tool "$OUT"_report.json
