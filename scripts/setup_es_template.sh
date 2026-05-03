#!/bin/bash
# Elasticsearch index template 설정 스크립트
# logs-* 인덱스에 적용될 기본 설정을 template으로 등록
# → 컨테이너 재시작 후 새 인덱스가 생성될 때도 자동으로 설정 유지

ES_HOST="localhost:9200"
TEMPLATE_NAME="logs-template"

echo "🔍 Elasticsearch 연결 확인 중..."
until curl -s "$ES_HOST/_cluster/health" > /dev/null; do
  echo "  대기 중..."
  sleep 2
done
echo "✅ 연결 성공!"

echo ""
echo "📋 index template 적용 중..."

curl -s -X PUT "$ES_HOST/_index_template/$TEMPLATE_NAME" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["logs-*"],
    "template": {
      "settings": {
        "refresh_interval": "5s",
        "number_of_shards": 1,
        "number_of_replicas": 0
      }
    },
    "priority": 1
  }' | python3 -m json.tool

echo ""
echo "✅ template 적용 완료! 이후 생성되는 logs-* 인덱스에 자동 적용됩니다."
echo ""
echo "📌 현재 이미 존재하는 인덱스에도 즉시 적용하려면:"
echo "   curl -X PUT \"$ES_HOST/logs-*/_settings\" \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"index\": {\"refresh_interval\": \"5s\"}}'"
