#!/usr/bin/env python3
"""
ELK 스택 부하 테스트 스크립트
Phase 1의 한계를 찾아서 Phase 2의 필요성을 체감하기 위한 도구

[v2] 비동기(asyncio + aiohttp) 전환
- 기존: 동기 requests + sleep → 응답 대기 시간이 interval을 잠식해 목표 처리율 미달
- 변경: 각 워커가 asyncio.sleep 으로 interval을 정확히 지키며 비동기 요청
"""

import asyncio
import aiohttp
import time
import random
import threading
from datetime import datetime
from collections import deque
import statistics

# 설정
LOGSTASH_URL = "http://localhost:5044"
LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
SERVICES = ["web-app", "api-server", "worker", "auth-service", "payment", "notification"]

# 통계 수집
stats = {
    "sent": 0,
    "failed": 0,
    "latencies": deque(maxlen=1000),  # 최근 1000개만 유지
    "start_time": None
}
stats_lock = asyncio.Lock()  # 비동기 환경에서 stats 보호용

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

async def fire_request(session):
    """단건 요청 발사 - 응답 처리만 담당 (워커와 분리)"""
    try:
        log = generate_log()
        start = time.time()

        async with session.post(
            LOGSTASH_URL,
            json=log,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as response:
            latency = (time.time() - start) * 1000  # ms

            if response.status == 200:
                async with stats_lock:
                    stats["sent"] += 1
                    stats["latencies"].append(latency)
            else:
                async with stats_lock:
                    stats["failed"] += 1

    except Exception:
        async with stats_lock:
            stats["failed"] += 1

async def send_log_worker(session, worker_id, target_rate, duration):
    """비동기 워커 - interval마다 요청을 fire-and-forget으로 발사"""
    end_time = time.time() + duration
    interval = 1.0 / target_rate if target_rate > 0 else 0

    while time.time() < end_time:
        # 응답을 기다리지 않고 발사만 함 → interval이 응답 대기에 잠식되지 않음
        asyncio.create_task(fire_request(session))

        if interval > 0:
            await asyncio.sleep(interval)

def print_stats():
    """실시간 통계 출력 (별도 스레드에서 실행)"""
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
        print(f"✓ 성공: {stats['sent']:,}건")
        print(f"✗ 실패: {stats['failed']:,}건")
        print(f"📈 처리율: {rate:.1f} 건/초")
        print(f"⏱️  평균 지연: {avg_latency:.1f}ms")
        print(f"⏱️  P95 지연: {p95_latency:.1f}ms")
        print(f"⏱️  P99 지연: {p99_latency:.1f}ms")

        if stats["failed"] > stats["sent"] * 0.1:  # 실패율 10% 이상
            print(f"\n⚠️  경고: 실패율이 높습니다! ({stats['failed']/max(stats['sent'],1)*100:.1f}%)")
            print(f"    Elasticsearch가 부하를 감당하지 못하는 것 같습니다.")

        if p99_latency > 1000:  # P99 지연 1초 이상
            print(f"\n⚠️  경고: 응답 시간이 느립니다! (P99: {p99_latency:.1f}ms)")
            print(f"    Logstash 처리 속도가 병목일 수 있습니다.")

def check_logstash_connection():
    """Logstash 연결 상태 확인 (동기 - 시작 전 1회만 호출)"""
    import requests as req
    try:
        print("🔍 Logstash 연결 확인 중...")
        req.get("http://localhost:9600", timeout=3)
        print("✅ Logstash 연결 성공!")
        return True
    except req.exceptions.ConnectionError:
        print("❌ Logstash에 연결할 수 없습니다!")
        print("\n해결 방법:")
        print("1. docker-compose ps 로 컨테이너 상태 확인")
        print("2. docker-compose logs logstash 로 로그 확인")
        print("3. Logstash가 시작되려면 30초~1분 정도 걸립니다")
        return False
    except Exception as e:
        print(f"⚠️  연결 확인 중 오류: {e}")
        return False

async def run_load_test(num_workers, target_rate_per_worker, duration):
    """비동기 부하 테스트 실행"""

    # Logstash 연결 확인 (동기)
    if not check_logstash_connection():
        print("\n💡 먼저 ELK 스택을 시작하세요:")
        print("   docker-compose up -d")
        return

    print("="*60)
    print("🚀 ELK 스택 부하 테스트 시작 (비동기 모드)")
    print("="*60)
    print(f"워커 코루틴: {num_workers}개")
    print(f"워커당 목표 처리율: {target_rate_per_worker} 건/초")
    print(f"총 목표 처리율: {num_workers * target_rate_per_worker} 건/초")
    print(f"테스트 시간: {duration}초")
    print("="*60)
    print("\nCtrl+C를 눌러 중간에 종료할 수 있습니다.\n")

    stats["start_time"] = time.time()

    # 통계 출력 스레드 (별도 스레드 - asyncio 루프 블로킹 방지)
    stats_thread = threading.Thread(target=print_stats, daemon=True)
    stats_thread.start()

    # aiohttp 세션 1개 공유 (커넥션 풀 활용)
    connector = aiohttp.TCPConnector(limit=num_workers * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 모든 워커 코루틴을 동시에 실행
        tasks = [
            send_log_worker(session, i, target_rate_per_worker, duration)
            for i in range(num_workers)
        ]
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\n\n🛑 테스트 중단됨")

    # 워커 종료 후 아직 처리 중인 fire-and-forget 요청 완료 대기
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    # 최종 리포트
    elapsed = time.time() - stats["start_time"]
    final_rate = stats["sent"] / elapsed

    print("\n\n" + "="*60)
    print("📋 최종 결과")
    print("="*60)
    print(f"총 전송 시도: {stats['sent'] + stats['failed']:,}건")
    print(f"✓ 성공: {stats['sent']:,}건")
    print(f"✗ 실패: {stats['failed']:,}건")
    print(f"📈 평균 처리율: {final_rate:.1f} 건/초")
    print(f"⏱️  테스트 시간: {elapsed:.1f}초")

    if stats["latencies"]:
        latencies = list(stats["latencies"])
        print(f"\n지연 시간 통계:")
        print(f"  평균: {statistics.mean(latencies):.1f}ms")
        print(f"  중앙값: {statistics.median(latencies):.1f}ms")
        print(f"  P95: {statistics.quantiles(latencies, n=20)[18]:.1f}ms")
        print(f"  P99: {statistics.quantiles(latencies, n=100)[98]:.1f}ms")
        print(f"  최대: {max(latencies):.1f}ms")

    print("\n💡 분석:")

    total_attempts = stats["sent"] + stats["failed"]
    if total_attempts == 0:
        print(f"  ⚠️  로그 전송 실패 - Logstash 연결을 확인하세요!")
        print(f"  → http://localhost:5044 에 접근 가능한가요?")
        print(f"  → docker-compose ps 로 컨테이너 상태를 확인해보세요")
        return

    success_rate = (stats["sent"] / total_attempts) * 100

    if success_rate < 95:
        print(f"  ⚠️  성공률 {success_rate:.1f}% - 시스템이 부하를 감당하지 못합니다")
        print(f"  → Phase 2에서 Kafka 버퍼링이 필요합니다!")
    elif final_rate < num_workers * target_rate_per_worker * 0.8:
        print(f"  ⚠️  목표 처리율의 {final_rate/(num_workers * target_rate_per_worker)*100:.1f}%만 달성")
        print(f"  → 현재 아키텍처의 한계입니다. 확장이 필요합니다.")
    else:
        print(f"  ✅ 목표 달성! 이 정도 부하는 처리 가능합니다.")
        print(f"  → 더 높은 부하로 테스트해보세요!")

    print("="*60)

def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║           ELK Stack 부하 테스트 & 병목 분석 도구              ║
╚═══════════════════════════════════════════════════════════════╝

이 스크립트로 Phase 1의 한계를 찾아봅시다!

테스트 시나리오:
1. 🟢 가벼운 부하 (5 workers × 10 req/s = 50 req/s)
2. 🟡 중간 부하 (10 workers × 50 req/s = 500 req/s)  
3. 🔴 무거운 부하 (20 workers × 100 req/s = 2000 req/s)
4. 💥 한계 테스트 (50 workers × 100 req/s = 5000 req/s)
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
        asyncio.run(run_load_test(workers, rate, duration))
    elif choice == "5":
        try:
            workers = int(input("워커 코루틴 수: "))
            rate = int(input("워커당 처리율 (건/초): "))
            duration = int(input("테스트 시간 (초): "))
            print(f"\n🎯 커스텀 테스트를 시작합니다...\n")
            time.sleep(2)
            asyncio.run(run_load_test(workers, rate, duration))
        except ValueError:
            print("❌ 잘못된 입력입니다.")
    else:
        print("❌ 잘못된 선택입니다.")

if __name__ == "__main__":
    main()