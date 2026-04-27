### 1️⃣ ELK 스택 시작

```bash
docker-compose up -d
docker-compose ps

# 로그 확인
docker-compose logs -f
```

### 2️⃣ 서비스 확인

- **Elasticsearch**: http://localhost:9200

- **Kibana**: http://localhost:5601

- **Logstash**: http://localhost:9600

### 3️⃣ 로그 쌓아보기

Python 스크립트 실행:

```bash
# Python requests 라이브러리 설치
pip install requests

# 로그 생성 스크립트 실행
python3 scripts/send_logs.py
```

## 🎪 테스트 리스트

### 부하 테스트
```python
# send_logs.py 수정해서 sleep 시간 줄이기
time.sleep(0.01)  # 초당 100개 로그!
```


### 필터링 테스트
`logstash/pipeline/logstash.conf` 파일 수정:

```ruby
filter {
  # ERROR 로그만 저장
  if [level] != "ERROR" {
    drop { }
  }
}
```

수정 후:
```bash
docker-compose restart logstash
```

### 인덱스 확인
```bash
# Elasticsearch에 저장된 인덱스 확인
curl http://localhost:9200/_cat/indices?v

# 특정 로그 검색
curl http://localhost:9200/logs-*/_search?pretty
```

## 🛠️ 문제 확인하기 ㅇㅂㅇ

### Elasticsearch가 안 뜨는 경우
```bash
# 메모리 부족이면 docker-compose.yml에서 ES_JAVA_OPTS를 -Xms256m -Xmx256m으로 줄이기
docker-compose down
docker-compose up -d
```

### Logstash가 안 뜨는 경우
```bash
# 설정 파일 문법 확인
docker-compose logs logstash
```

### 로그가 Kibana에 안 보이는 경우
```bash
# Elasticsearch에 데이터가 들어옴?
curl http://localhost:9200/logs-*/_count

# Logstash가 제대로 처리함?
docker-compose logs logstash | tail -20
```

Ref) 
- Elasticsearch 공식 문서: https://www.elastic.co/guide/
- Kibana 시작하기: https://www.elastic.co/guide/en/kibana/current/get-started.html
- Logstash 설정: https://www.elastic.co/guide/en/logstash/current/configuration.html
