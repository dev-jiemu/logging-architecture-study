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

echo "💡 분석 가이드:"
echo "--------------------------------"
echo "1. CPU가 100%에 가까운 컨테이너 = 병목!"
echo "2. Logstash CPU 100% → Logstash 확장 필요 (Phase 2)"
echo "3. Elasticsearch CPU 100% → ES 클러스터링 필요 (Phase 3)"
echo "4. 메모리 부족 → 힙 사이즈 증가 필요"
echo "5. 인덱싱 속도 < 전송 속도 → Kafka 버퍼링 필요!"