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
## 2026.05.03 아래 내용들은 claude 한테 정리해달라고 했음 🤔
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

### 4. Elasticsearch 튜닝 - refresh_interval 조정

#### refresh_interval 이란?
ES는 새로 인덱싱된 문서를 검색 가능하게 만들기 위해 주기적으로 **refresh** 작업을 수행함  
이 작업은 메모리의 데이터를 디스크 세그먼트로 flush 하는 비용이 드는 작업이라, 주기가 짧을수록 인덱싱 처리량에 영향을 줌

#### 값 선택 기준
| 값 | 특징 | 적합한 상황 |
|--|--|--|
| `1s` (기본값) | 실시간에 가까움, ES 부하 높음 | 실시간 모니터링 |
| `5s` | 약간의 지연, 부하 완화 | 로그 모니터링 일반적 추천 |
| `30s` | 지연 큼, 부하 많이 완화 | 배치성 인덱싱 |

#### 5s로 설정한 이유
이 프로젝트는 API 요청 로그를 실시간으로 수집하는 구조임  
- `30s`: 장애 발생 시 30초 전 로그만 보이는 상황은 모니터링 관점에서 너무 치명적
- `1s` (기본값): refresh 작업 자체가 매초 발생해서 높은 부하에서는 인덱싱 처리량을 잠식할 수 있음
- `5s`: 5초 지연은 실시간 모니터링에 큰 지장 없으면서 refresh 빈도도 낮춰 부하 완화 가능 → **그래서 5s로 결정**

#### 적용 방법
매번 수동으로 API 호출하지 않고, `docker-compose.yml`에 `es-setup` 초기화 컨테이너를 추가해서  
`docker-compose up` 시 ES가 준비되면 자동으로 index template을 등록하도록 구성

```yaml
es-setup:
  image: curlimages/curl:latest
  restart: "no"   # 한 번 실행 후 종료
  # logs-* 패턴 인덱스에 refresh_interval: 5s 자동 적용
```

index template 방식이라 이후 `logs-2026.05.03` 처럼 날짜별로 새 인덱스가 생성될 때도 자동 적용됨

---

### 5. 단일 ELK 스택 한계 테스트

load_test.py v3 (fire-and-forget) + refresh_interval 5s 상태에서 부하를 단계적으로 높여 한계점 탐색

| 부하 단계 | 목표 | 실제 처리율 | 실패 | 평균 지연 | P99 | 결과 |
|--|--|--|--|--|--|--|
| 중간 | 500 req/s | 472 req/s | 0건 | 2.9ms | 8.6ms | ✅ |
| 무거운 | 2,000 req/s | 1,884 req/s | 0건 | 2.1ms | 5.5ms | ✅ |
| 한계 | 5,000 req/s | 4,347 req/s | 0건 | 1.0ms | 5.6ms | ✅ |
| 초과 | 10,000 req/s | **5,117 req/s** | **19건** | **24.3ms** | **58.1ms** | ⚠️ |

**약 5,000~5,100 req/s 부근이 현재 단일 ELK 스택의 한계**  
10,000 req/s 구간에서 세 가지 한계 신호가 동시에 발생:
- 실패 건수 발생 (로그 유실)
- 평균 지연 2ms → 24ms로 **12배** 급등
- 목표 달성률 51%로 하락

#### Phase 2 Kafka 도입 필요성
5,000 req/s 이상이 들어오면 ELK가 받아내지 못하고 유실이 발생함  
Kafka를 앞단에 두면 버퍼 역할을 해서 ELK가 자기 페이스로 소화할 수 있게 됨

```
# 현재 (단일 ELK)
[load_test] --5,000+ req/s--> [Logstash] --> [ES]
                                  ↑ 여기서 터짐

# Phase 2 (Kafka 도입)
[load_test] --5,000+ req/s--> [Kafka 버퍼] --> [Logstash] --> [ES]
                                    ↑                ↑
                              다 받아둠        자기 페이스(~5,000 req/s)로 소화
```

## Phase 2 : Kafka
도입 전에, queue 나 redis 등 비교해봄

| | Kafka | RabbitMQ | Redis Streams | Pulsar |
|--|--|--|---------------|--|
| 디스크 보관 | ✅ | ✅ | ❌ 메모리임..;;    | ✅ |
| 재처리 | ✅ offset | ❌ | ✅ offset      | ✅ offset |
| 다중 소비자 | ✅ | △ 별도 큐 필요 | ✅             | ✅ |
| 대용량 로그 | ✅ | △ | ❌             | ✅ |
| 운영 복잡도 | 중간 | 낮음 | 낮음            | 높음 |
| 로그 파이프라인 적합성 | **⭐ 표준** | 부적합 | 소규모만          | 대안 가능 |