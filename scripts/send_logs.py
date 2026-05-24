#!/usr/bin/env python3
"""
간단한 로그 생성기
Kafka로 로그를 전송합니다 (기존: Logstash HTTP 직접 전송)

[변경 이유]
- 기존: requests.post → Logstash HTTP → ES (동기, 직접 전송)
- 변경: KafkaProducer → Kafka Topic → Logstash Consumer → ES
- 효과: 트래픽 스파이크 시 Kafka가 버퍼링하므로 로그 유실 없음

[설치]
pip install kafka-python
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka 브로커 주소 (로컬 테스트용)
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "logs"

# 로그 레벨
LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]

# 샘플 메시지
MESSAGES = [
    "User logged in successfully",
    "Database connection established",
    "Cache miss for key: user_session",
    "API request processed",
    "File uploaded successfully",
    "Payment transaction completed",
    "Email sent to user",
    "Background job started",
    "Error processing request",
    "Connection timeout",
]

def create_producer():
    """Kafka Producer 생성"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=16384,       # 배치 사이즈 (bytes) - 작은 메시지 묶어서 전송
        linger_ms=5,            # 배치 대기 시간 - 5ms 동안 모아서 전송
        compression_type="gzip" # 압축으로 네트워크 비용 절감
    )

def generate_log():
    """랜덤 로그 생성"""
    log = {
        "timestamp": datetime.now().isoformat(),
        "level": random.choice(LOG_LEVELS),
        "message": random.choice(MESSAGES),
        "service": random.choice(["web-app", "api-server", "worker", "auth-service"]),
        "user_id": f"user_{random.randint(1, 1000)}",
        "request_id": f"req_{random.randint(10000, 99999)}",
        "duration_ms": random.randint(10, 500),
    }
    return log

def send_log(producer, log):
    """Kafka로 로그 전송 (비동기 - flush 없이 배치 전송)"""
    try:
        future = producer.send(TOPIC, value=log)
        print(f"✓ Sent: [{log['level']}] {log['message']}")
        return future
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def main():
    print("🚀 로그 생성 시작! (Kafka 전송 모드)")
    print("Ctrl+C를 눌러 종료하세요\n")

    producer = create_producer()

    try:
        count = 0
        while True:
            log = generate_log()
            send_log(producer, log)
            count += 1

            if count % 10 == 0:
                producer.flush()  # 10개마다 한 번씩 배치 강제 전송
                print(f"\n📊 총 {count}개 로그 전송됨\n")

            # 1초에 1~3개 로그 생성 (랜덤 간격)
            time.sleep(random.uniform(0.3, 1.0))

    except KeyboardInterrupt:
        producer.flush()  # 남은 배치 전송
        producer.close()
        print(f"\n\n✅ 종료! 총 {count}개의 로그를 전송했습니다.")

if __name__ == "__main__":
    main()
