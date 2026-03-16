# 리팩터링 설계안 (최신 코드 기준)

## 목표
- 동시성 안전성 유지(writeLock 일원화), 버퍼 누락 탐지 보강
- 예외·로그 정비(UTF-8, 사용자/로그 분리), 입력 검증 일관화
- 설정/스토어 안정성(경로 검증·옵션 생성, temp 헬퍼)
- 관측성 강화(통계 스냅샷/리셋, MDC 가이드)

## 현황 요약
- API는 `producerId`(write) / `consumerId`(read/commit) 사용, 기본 오버로드는 thread name 기반 제공
- Stable 모드: consumer별 `readBufferMap`으로 커밋 전 데이터 격리
- 단일 큐 구조, 키 재정렬 1000회 연산 주기

## 개선 포인트
- 버퍼 TTL: 데이터 처리 경로에서는 TTL 초과 시 로그만 남기고 정상 처리; 프로세스 재기동/초기화 시 TTL 초과 엔트리를 정리
- 예외/로그: 메시지 UTF-8 정비, 사용자 메시지 vs 내부 로그 분리, 파라미터 검증 일관화
- 설정/스토어: 경로 검증·옵션화된 자동 생성, temp 파일 헬퍼/정리, close 재시도 로직
- 통계/관측성: executorId 스냅샷/리셋 API, MDC 가이드
- 문서/샘플: README/USAGE/MIGRATION 점검 및 업데이트

## 상세 실행 계획
### 1) 준비/정리
- 깨진 메시지 UTF-8 통일
- 기본 read/write, stable 버퍼, 롤백 경로 단위 테스트 추가
- LoadTest 기반 멀티스레드 스모크

### 2) 동시성·버퍼(TTL) 강화
- `readBufferMap` 값 래핑: `BufferEntry{data, lastAccessEpochMillis}`
- 읽기 시: TTL 초과면 warn 로그만 남기고 데이터는 그대로 반환/처리 (정합성 유지)
- 재기동 시 초기화: TTL 초과 엔트리는 로그 남기고 삭제해 파일 누적 방지

```java
// readBuffer 시 TTL 체크 (데이터는 정상 반환)
BufferEntry entry = readBufferMap.get(consumerId);
if (entry != null && now - entry.getLastAccessEpochMillis() > ttl) {
    logger.warn("buffer ttl exceeded queue={} consumer={} size={}", q, consumerId, size(entry));
    // 데이터는 그대로 반환
}
return entry == null ? null : entry.getData();
```

```java
// 초기화 시 TTL 초과 엔트리 정리 (QueueManager init 등)
for (var kv : readBufferMap.entrySet()) {
    if (now - kv.getValue().getLastAccessEpochMillis() > ttl) {
        logger.warn("buffer expired on init queue={} consumer={} size={}", q, kv.getKey(), size(kv.getValue()));
        readBufferMap.remove(kv.getKey());
    }
}
```

### 3) 예외·로그 정비
- `Queue*Exception` 메시지 정돈, 사용자 메시지 vs 내부 로그 분리
- 입력 검증 일관화: null/빈 문자열 즉시 예외, 메시지에 queueName 포함

### 4) 설정/스토어 유틸
- 경로 검증 + `autoCreateDirs` 플래그(기본 off)
- `ControllerFactory` temp 파일 생성/정리 헬퍼
- close 시 lock 재시도/로그 정리

### 5) 통계/관측성
- `StatsSnapshot` DTO, `resetStats()` 추가, executorId 집계 유지
- MDC 가이드: `%X{producerId}`/`%X{consumerId}` 패턴 예시 문서화

### 6) 문서/샘플
- README/USAGE/MIGRATION/샘플을 최신 API와 TTL 정책 설명으로 업데이트

## 테스트 계획
- 단위: stable 버퍼 TTL 경고 발생 여부, 기본 read/write, bulk commit+rollback, 예외 메시지 검증
- 통합: temp 파일 기반 end-to-end, 기본/명시 ID 혼용 시나리오
- 동시성: LoadTest 변형(다중 producer/consumer), TTL 초과 후 초기화 정리 시나리오

## 리스크 및 대응
- 호환성: 기존 API 유지, 내부 타입 변경(BufferEntry) 시 직렬화 호환성 확인
- 동시성 회귀: 멀티스레드 테스트 CI 포함
- 스토어 부작용: 경로 자동 생성은 기본 off, TTL 삭제는 초기화 시에만 수행(런타임은 로그만)

## 완료 기준
- 코드: 버퍼 TTL 경고/정리, 예외·로그 정비, 설정 유틸, 통계 스냅샷/리셋 반영
- 테스트: 신규 단위/통합/동시성 테스트 통과
- 문서: README/USAGE/MIGRATION/샘플 최신화 및 본 설계 반영
