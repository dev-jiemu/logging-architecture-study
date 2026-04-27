#!/usr/bin/env python3
"""
간단한 로그 생성기
ELK 스택으로 로그를 전송합니다
"""

import requests
import json
import time
import random
from datetime import datetime

# Logstash HTTP 엔드포인트
LOGSTASH_URL = "http://localhost:5044"

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

def send_log(log):
    """Logstash로 로그 전송"""
    try:
        response = requests.post(
            LOGSTASH_URL,
            json=log,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if response.status_code == 200:
            print(f"✓ Sent: [{log['level']}] {log['message']}")
        else:
            print(f"✗ Failed: {response.status_code}")
    except Exception as e:
        print(f"✗ Error: {e}")

def main():
    print("🚀 로그 생성 시작!")
    print("Ctrl+C를 눌러 종료하세요\n")
    
    try:
        count = 0
        while True:
            log = generate_log()
            send_log(log)
            count += 1
            
            if count % 10 == 0:
                print(f"\n📊 총 {count}개 로그 전송됨\n")
            
            # 1초에 1~3개 로그 생성 (랜덤 간격)
            time.sleep(random.uniform(0.3, 1.0))
            
    except KeyboardInterrupt:
        print(f"\n\n✅ 종료! 총 {count}개의 로그를 전송했습니다.")

if __name__ == "__main__":
    main()
