# 테스트 목록

이 문서는 현재 `mvn test` 대상 JUnit 테스트 목록과 목적을 정리한다.

## com.queue.file.controller
- BaseControllerManagementTest: 관리 메서드(데이터/버퍼/캐시/헬퍼) 동작 확인
- BaseControllerTest: 기본 write/read/commit 및 요약 조회 흐름 확인
- BufferTtlTest: stable mode 버퍼 TTL 만료 처리 및 초기 정리 확인
- ControllerFactoryBestEffortTest: 경로 쓰기 불가/롤백 경로 best‑effort 확인 (환경 의존 스킵 가능)
- ControllerFactoryCustomConfigTest: 커스텀 설정 기반 컨트롤러 생성 smoke 테스트
- ControllerFactoryTest: 기본/안정/벌크 모드 생성 및 기본 IO 확인
- DataAccessBehaviorTest: read/write 경계값, 버퍼 우선, 캐시 관련 동작 확인
- DataAccessBulkCommitBestEffortTest: bulk commit commit/dirty 상태 best‑effort 확인 (환경 의존 스킵 가능)
- DataAccessTest: 기본 read/write/commit 및 동기화 확인
- ExceptionHandlingTest: 예외 전파 및 메시지 템플릿 확인
- QueueManagerCheckStateTest: store 파일 누락 시 상태 검증 예외 확인
- QueueManagerMapClosedTest: map close 상태 예외 확인 (환경 의존 스킵 가능)
- QueueManagerRealignTest: realign 주기 동작 확인
- QueueManagerTest: 상태 점검 예외 기본 케이스 확인
- StatsTrackerInvokeTest: invoke 기록 저장 확인
- StatsTrackerTest: 카운트 집계 및 DataAccess 연동 확인

## com.queue.file.vo
- BufferEntryTest: touch 동작(타임스탬프/참조) 확인
- FileQueueConfigVoTest: 경로 파싱 및 조합 로직 확인
- InOutStorageOverflowTest: 오버플로우 방어 로직 확인
- QueueSummaryVoTest: toString 포맷 확인
- StoreInfoCloseAllTest: 스토어 closeAll 동작 확인
