#!/bin/bash
# 병목 위치 찾기 스크립트
# load_test.py 돌리는 중에 이걸 실행하세요!

echo "🔍 ELK 스택 병목 분석 시작..."
echo "================================"
echo ""

echo "📊 1. 컨테이너 리소스 사용량"
echo "--------------------------------"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
echo ""

echo "📊 2. Logstash 처리 통계"
echo "--------------------------------"
curl -s http://localhost:9600/_node/stats/pipelines | jq '.pipelines.main.events'
echo ""

echo "📊 3. Elasticsearch 상태"
echo "--------------------------------"
curl -s http://localhost:9200/_cluster/health?pretty | jq '{status, number_of_nodes, active_shards, unassigned_shards}'
echo ""

echo "📊 4. Elasticsearch 인덱싱 통계"
echo "--------------------------------"
curl -s http://localhost:9200/_stats | jq '.indices."logs-*".total.indexing'
echo ""

echo "📊 5. 실시간 로그 유입 (10초간)"
echo "--------------------------------"
START=$(curl -s http://localhost:9200/logs-*/_count | jq '.count')
echo "현재 로그 수: $START"
sleep 10
END=$(curl -s http://localhost:9200/logs-*/_count | jq '.count')
echo "10초 후 로그 수: $END"
RATE=$(( ($END - $START) / 10 ))
echo "실제 인덱싱 속도: ${RATE} 건/초"
echo ""

echo "📊 6. Kafka 브로커 상태"
echo "--------------------------------"
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 \
  && echo "✅ Kafka 브로커 정상 동작 중" \
  || echo "❌ Kafka 브로커 응답 없음"
echo ""

echo "📊 7. Kafka Topic 목록 및 파티션"
echo "--------------------------------"
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic logs 2>/dev/null \
  || echo "⚠️  logs 토픽이 아직 생성되지 않았습니다. (첫 메시지 전송 후 자동 생성)"
echo ""

echo "📊 8. Kafka Consumer Group Lag (핵심 지표!)"
echo "--------------------------------"
# lag = Kafka에 쌓여있지만 Logstash가 아직 처리 못한 메시지 수
# lag이 계속 증가 → Logstash/ES가 병목
# lag이 0에 수렴 → 정상 처리 중
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group logstash-consumer 2>/dev/null \
  || echo "⚠️  logstash-consumer 그룹이 아직 없습니다. (Logstash 기동 후 생성됩니다)"
echo ""

echo "📊 9. Kafka Topic 오프셋 (전체 누적 메시지 수)"
echo "--------------------------------"
# LOG-END-OFFSET = Producer가 쌓은 총 메시지 수
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic logs \
  --time -1 2>/dev/null \
  || echo "⚠️  logs 토픽 오프셋 조회 실패 (토픽 미생성 또는 Kafka 미기동)"
echo ""

echo "📊 10. Kafka 실시간 Lag 추이 (10초간)"
echo "--------------------------------"
# 10초 간격으로 lag 2회 측정 → 증가 추세면 Logstash가 따라가지 못하는 것
get_lag() {
  docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --group logstash-consumer 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}
LAG_START=$(get_lag)
echo "현재 Lag: ${LAG_START} 건"
sleep 10
LAG_END=$(get_lag)
echo "10초 후 Lag: ${LAG_END} 건"
if [ "$LAG_END" -gt "$LAG_START" ] 2>/dev/null; then
  echo "⚠️  Lag 증가 중 (+$(( LAG_END - LAG_START ))건) → Logstash/ES 처리 속도가 Producer보다 느립니다"
elif [ "$LAG_END" -eq 0 ] 2>/dev/null; then
  echo "✅ Lag 0 → Logstash가 실시간으로 따라가고 있습니다"
else
  echo "✅ Lag 감소 중 → Kafka 버퍼 소화 중"
fi
echo ""

echo "💡 분석 가이드:"
echo "--------------------------------"
echo "1. CPU가 100%에 가까운 컨테이너 = 병목!"
echo "2. Logstash CPU 100% → consumer_threads 증가 또는 Logstash 스케일아웃 필요"
echo "3. Elasticsearch CPU 100% → ES 클러스터링 필요"
echo "4. 메모리 부족 → 힙 사이즈 증가 필요"
echo ""
echo "🔑 Kafka Lag 해석:"
echo "   Lag = 0          → 정상, Logstash가 실시간 처리 중"
echo "   Lag 소폭 유지     → Kafka 버퍼링 동작 중 (정상)"
echo "   Lag 지속 증가     → Logstash or ES 병목! consumer_threads 늘리거나 ES 확장 필요"
echo "   Kafka 조회 불가  → docker-compose up -d kafka zookeeper 로 먼저 기동하세요"