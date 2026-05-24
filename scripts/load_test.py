#!/usr/bin/env python3
"""
ELK + Kafka 부하 테스트 스크립트
Kafka Producer를 통해 대용량 로그를 전송하고 처리 성능을 측정합니다.

[변경 이유]
- 기존: asyncio + aiohttp → Logstash HTTP 직접 전송
- 변경: KafkaProducer (멀티스레드) → Kafka Topic → Logstash Consumer → ES
- 핵심 차이: Kafka는 Producer 전송 속도와 Consumer 처리 속도를 분리
             → Producer는 Kafka에 넣는 속도만 측정, ES 처리 속도에 영향 안 받음

[설치]
pip install kafka-python
"""

import time
import random
import threading
from datetime import datetime
from collections import deque
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import statistics

# 설정
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "logs"
LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
SERVICES = ["web-app", "api-server", "worker", "auth-service", "payment", "notification"]

# 통계 수집
stats = {
    "sent": 0,
    "failed": 0,
    "latencies": deque(maxlen=1000),
    "start_time": None
}
stats_lock = threading.Lock()

def generate_log():
    """로그 생성"""
    return {
        "timestamp": datetime.now().isoformat(),
        "level": random.choice(LOG_LEVELS),
        "message": f"Request processed with status {random.choice([200, 201, 400, 404, 500])}",
        "service": random.choice(SERVICES),
        "user_id": f"user_{random.randint(1, 10000)}",
        "request_id": f"req_{random.randint(100000, 999999)}",
        "duration_ms": random.randint(10, 2000),
        "endpoint": f"/api/v1/{random.choice(['users', 'orders', 'products', 'auth'])}",
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
    }

def create_producer():
    """Kafka Producer 생성 (성능 최적화 설정)"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=65536,       # 64KB 배치 - 처리량 극대화
        linger_ms=10,           # 10ms 대기 후 배치 전송
        compression_type="gzip",
        acks=1,                 # leader ack만 받음 (속도 우선, 내구성 일부 포기)
        retries=3               # 실패 시 재시도
    )

def on_send_success(record_metadata, start_time):
    """전송 성공 콜백"""
    latency = (time.time() - start_time) * 1000
    with stats_lock:
        stats["sent"] += 1
        stats["latencies"].append(latency)

def on_send_error(exc):
    """전송 실패 콜백"""
    with stats_lock:
        stats["failed"] += 1

def send_worker(producer, worker_id, target_rate, duration):
    """워커 스레드 - 목표 처리율에 맞춰 Kafka로 전송"""
    end_time = time.time() + duration
    interval = 1.0 / target_rate if target_rate > 0 else 0

    while time.time() < end_time:
        log = generate_log()
        start_time = time.time()
        try:
            producer.send(TOPIC, value=log).add_callback(
                on_send_success, start_time
            ).add_errback(on_send_error)
        except KafkaError as e:
            with stats_lock:
                stats["failed"] += 1

        if interval > 0:
            time.sleep(interval)

def print_stats():
    """실시간 통계 출력 (별도 스레드)"""
    while True:
        time.sleep(5)

        if stats["start_time"] is None:
            continue

        elapsed = time.time() - stats["start_time"]
        rate = stats["sent"] / elapsed if elapsed > 0 else 0

        latencies = list(stats["latencies"])
        if latencies:
            avg_latency = statistics.mean(latencies)
            p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 20 else max(latencies)
            p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) > 100 else max(latencies)
        else:
            avg_latency = p95_latency = p99_latency = 0

        print(f"\n{'='*60}")
        print(f"📊 실시간 통계 (경과: {elapsed:.1f}초)")
        print(f"{'='*60}")
        print(f"✓ Kafka 전송 성공: {stats['sent']:,}건")
        print(f"✗ 실패: {stats['failed']:,}건")
        print(f"📈 Producer 처리율: {rate:.1f} 건/초")
        print(f"⏱️  평균 지연(Kafka ack): {avg_latency:.1f}ms")
        print(f"⏱️  P95 지연: {p95_latency:.1f}ms")
        print(f"⏱️  P99 지연: {p99_latency:.1f}ms")
        print(f"\n💡 참고: 이 수치는 Kafka까지의 전송 지연입니다.")
        print(f"   Logstash → ES 처리는 별도로 Kibana에서 확인하세요.")

        if stats["failed"] > stats["sent"] * 0.1:
            print(f"\n⚠️  경고: 실패율이 높습니다! Kafka 연결을 확인하세요.")

def check_kafka_connection():
    """Kafka 연결 상태 확인"""
    try:
        print("🔍 Kafka 연결 확인 중...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=3000
        )
        producer.close()
        print("✅ Kafka 연결 성공!")
        return True
    except Exception as e:
        print(f"❌ Kafka에 연결할 수 없습니다! ({e})")
        print("\n해결 방법:")
        print("1. docker-compose ps 로 컨테이너 상태 확인")
        print("2. docker-compose up -d kafka zookeeper")
        print("3. Kafka가 시작되려면 20~30초 정도 걸립니다")
        return False

def run_load_test(num_workers, target_rate_per_worker, duration):
    """부하 테스트 실행 (멀티스레드 Kafka Producer)"""

    if not check_kafka_connection():
        print("\n💡 먼저 스택을 시작하세요:")
        print("   docker-compose up -d")
        return

    print("="*60)
    print("🚀 Kafka 부하 테스트 시작")
    print("="*60)
    print(f"워커 스레드: {num_workers}개")
    print(f"워커당 목표 처리율: {target_rate_per_worker} 건/초")
    print(f"총 목표 처리율: {num_workers * target_rate_per_worker} 건/초")
    print(f"테스트 시간: {duration}초")
    print("="*60)
    print("\nCtrl+C를 눌러 중간에 종료할 수 있습니다.\n")

    stats["start_time"] = time.time()

    # Producer는 스레드 간 공유 (thread-safe)
    producer = create_producer()

    # 통계 출력 스레드
    stats_thread = threading.Thread(target=print_stats, daemon=True)
    stats_thread.start()

    # 워커 스레드 실행
    workers = []
    for i in range(num_workers):
        t = threading.Thread(
            target=send_worker,
            args=(producer, i, target_rate_per_worker, duration),
            daemon=True
        )
        workers.append(t)
        t.start()

    try:
        for t in workers:
            t.join()
    except KeyboardInterrupt:
        print("\n\n🛑 테스트 중단됨")

    producer.flush()  # 남은 배치 전송
    producer.close()

    # 최종 리포트
    elapsed = time.time() - stats["start_time"]
    final_rate = stats["sent"] / elapsed if elapsed > 0 else 0

    print("\n\n" + "="*60)
    print("📋 최종 결과")
    print("="*60)
    print(f"총 전송 시도: {stats['sent'] + stats['failed']:,}건")
    print(f"✓ 성공 (Kafka 적재): {stats['sent']:,}건")
    print(f"✗ 실패: {stats['failed']:,}건")
    print(f"📈 평균 Producer 처리율: {final_rate:.1f} 건/초")
    print(f"⏱️  테스트 시간: {elapsed:.1f}초")

    if stats["latencies"]:
        latencies = list(stats["latencies"])
        print(f"\nKafka Producer 지연 통계:")
        print(f"  평균: {statistics.mean(latencies):.1f}ms")
        print(f"  중앙값: {statistics.median(latencies):.1f}ms")
        print(f"  P95: {statistics.quantiles(latencies, n=20)[18]:.1f}ms")
        print(f"  P99: {statistics.quantiles(latencies, n=100)[98]:.1f}ms")
        print(f"  최대: {max(latencies):.1f}ms")

    total_attempts = stats["sent"] + stats["failed"]
    success_rate = (stats["sent"] / total_attempts * 100) if total_attempts > 0 else 0

    print("\n💡 분석:")
    if success_rate >= 99:
        print(f"  ✅ Kafka 전송 성공률 {success_rate:.1f}% - 버퍼링 정상 동작!")
        print(f"  → Kibana에서 실제 ES 인덱싱 속도를 확인해보세요.")
    else:
        print(f"  ⚠️  성공률 {success_rate:.1f}% - Kafka 설정을 확인하세요.")

    print("="*60)

def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║        ELK + Kafka 부하 테스트 & 성능 측정 도구               ║
╚═══════════════════════════════════════════════════════════════╝

Kafka Producer 처리율을 측정합니다.
(ES 인덱싱 지연은 Logstash Consumer가 별도 처리)

테스트 시나리오:
1. 🟢 가벼운 부하 (5 workers × 10 req/s = 50 req/s)
2. 🟡 중간 부하 (10 workers × 50 req/s = 500 req/s)
3. 🔴 무거운 부하 (20 workers × 100 req/s = 2,000 req/s)
4. 💥 한계 테스트 (50 workers × 100 req/s = 5,000 req/s)
5. 🎯 커스텀 테스트

어떤 테스트를 실행하시겠습니까?
    """)

    choice = input("선택 (1-5): ").strip()

    scenarios = {
        "1": (5, 10, 60, "가벼운 부하"),
        "2": (10, 50, 60, "중간 부하"),
        "3": (20, 100, 60, "무거운 부하"),
        "4": (50, 100, 60, "한계 테스트"),
    }

    if choice in scenarios:
        workers, rate, duration, name = scenarios[choice]
        print(f"\n🎯 {name} 테스트를 시작합니다...\n")
        time.sleep(2)
        run_load_test(workers, rate, duration)
    elif choice == "5":
        try:
            workers = int(input("워커 스레드 수: "))
            rate = int(input("워커당 처리율 (건/초): "))
            duration = int(input("테스트 시간 (초): "))
            print(f"\n🎯 커스텀 테스트를 시작합니다...\n")
            time.sleep(2)
            run_load_test(workers, rate, duration)
        except ValueError:
            print("❌ 잘못된 입력입니다.")
    else:
        print("❌ 잘못된 선택입니다.")

if __name__ == "__main__":
    main()