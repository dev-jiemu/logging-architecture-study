# logging-architecture-study
로깅 시스템 아키텍처 연습해보기

---

### 1. ELK 구성 (feature/elk-stack-tutorial)
실제 실무에서 ELK 환경에서 내가 만든 어플리케이션의 로그 수집을 하고 있긴 한데, 해당 환경을 내가 구성한게 아니다보니...ㅇㅂㅇ.. <br>
겸사겸사 이해 해볼 겸 설정

![img.png](images/elasticsearch.png)

### 2. 수평 확장 공부해보기
load_test.py 돌려서 하나씩 체크해보는데, 중간 부하 버전으로만 해도 아래와 같은 결과를 얻음
```text
============================================================
📋 최종 결과
============================================================
총 전송 시도: 20,954건
✓ 성공: 20,954건
✗ 실패: 0건
📈 평균 처리율: 349.0 건/초
⏱️  테스트 시간: 60.0초

지연 시간 통계:
  평균: 6.6ms
  중앙값: 6.0ms
  P95: 12.0ms
  P99: 14.7ms
  최대: 18.1ms

💡 분석:
  ⚠️  목표 처리율의 69.8%만 달성
  → 현재 아키텍처의 한계입니다. 확장이 필요합니다.
============================================================
```
성공 자체는 다 했는데 전반적으로 지연됨이 확인됨
```text
NAME            CPU %     MEM USAGE / LIMIT
logstash        46.35%    753.6MiB / 7.667GiB
kibana          2.38%     655.6MiB / 7.667GiB
elasticsearch   24.52%    1.328GiB / 7.667GiB
```
컨테이너 자체는 그렇게 많이 쓰는것 같진 않고
```text
{
  "in": 24114,          // 들어온 로그
  "filtered": 23499,    // 필터 처리 완료
  "out": 23499,         // Elasticsearch로 전송
  "queue_push_duration_in_millis": 369,  // 큐 대기 시간 (짧음 = 좋음)
  "duration_in_millis": 42686  // 총 처리 시간
}
```
초당 처리량 = `23499` / `42.686` = 550 정도? <br>
엘라스틱서치만 보면 550 정도인데 실제 load_test 스크립트 돌린 결과에서 평균 처리율이 낮네...ㅇㅂㅇ <br>
```text
[load_test.py] --HTTP--> [Logstash] --> [Elasticsearch]
    331 req/s            550 req/s         353 req/s
```
logstach.conf 설정할때
```text
  http {
    port => 5044
    codec => json
  }
```
별도의 thread 설정을 주지 않았는데, 클로드에 말에 의하면 스레드 옵션을 별도로 주지 않으면 worker 개수 그대로 따라간다고 함 ㅇㅂㅇ

```text
jiemu@Jiemu-MacBook-Air  ~/IdeaProjects/logging-architecture-study   feature/elk-stack-tutorial ±✚  docker exec logstash nproc
8
```
스레드 2배로 늘리고 테스트 해봤는데 logstash 는 열심히 CPU 를 사용하는데 elasticsearch 가 변화 없고, 결정적으로 속도 병목도 별 차이가 없는거보니 다른 이유인듯?  <br>
=> 테스트를 위한 파이썬 스크립트가 동기로 구현되어 있어서, 비동기 형태로 바꾸니 어느정도 개선이 됨

---
아래 내용은 claude 한테 정리해달라고 했음 🤔
### 3. load_test.py 개선 과정 (측정 도구 자체의 구조 문제)

#### 문제 인식
ELK 스택 성능이 낮다고 생각했는데, 알고보니 **부하를 만들어내는 스크립트 자체가 병목**이었음

#### v1 → v2: 동기 → 비동기 전환 (asyncio + aiohttp)
기존 동기 방식의 문제:
```
요청 → 응답 대기(6ms) → sleep(20ms) → 요청 → ...
→ 1 사이클 = 26ms → 워커당 최대 38 req/s → 10워커 = 380 req/s 한계
```
`requests` 라이브러리를 `aiohttp`로, `threading`을 `asyncio`로 교체하니 응답 대기 시간이 줄었지만 여전히 sleep이 interval을 잠식하는 구조는 동일

#### v2 → v3: Fire-and-Forget 패턴 적용
**Fire-and-Forget** = "쏘고 잊어버린다"는 뜻으로, 요청을 보낸 후 응답을 기다리지 않고 바로 다음 작업으로 넘어가는 패턴

```
# v2 구조 (여전히 응답 대기가 interval을 잠식)
워커: [요청 → 응답 대기(3ms) → sleep(20ms) → 요청 → ...]
→ 1 사이클 = 23ms → 워커당 43 req/s 한계

# v3 구조 (Fire-and-Forget)
워커:    [발사 → sleep(20ms) → 발사 → sleep(20ms) → ...]  ← interval 정확히 유지
응답처리: [   fire_request 태스크가 백그라운드에서 알아서 완료   ]
→ 1 사이클 = 20ms → 워커당 50 req/s → 10워커 = 500 req/s 달성
```

`asyncio.create_task(fire_request(session))` 로 요청을 백그라운드 태스크로 던지고,
워커는 응답을 기다리지 않고 바로 `asyncio.sleep(interval)` 로 넘어가는 구조

#### 결과 비교
| | v1 동기 | v2 비동기 | v3 Fire-and-Forget |
|--|--|--|--|
| 처리율 | 349 req/s | 395 req/s | **473 req/s ✅** |
| 평균 지연 | 6.6ms | 3.7ms | 3.0ms |
| P99 | 14.7ms | 9.5ms | 8.8ms |
| 목표 달성 | 69.8% | 79.0% | **목표 달성** |
---