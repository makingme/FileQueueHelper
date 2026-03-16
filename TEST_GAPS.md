# 테스트 누락 목록 및 우선순위 (FileQueueHelper)

현재 `src/test/java/com/queue/file/controller` 기준으로, 누락된 테스트를 우선순위와 함께 정리했다.

## 우선순위 P0 (핵심 안정성/데이터 유실 위험)
- DataAccess.read/write 흐름의 오류 처리: `storeOpenTime` 미설정, dataMap 닫힘/NULL 등에서 `QueueReadException`/`QueueWriteException` 전파 확인.
- DataAccess.extractData: dataMap과 transactionKeyList의 동기화 보장(키 제거 일관성).
- DataAccess.readCommit: 버퍼 존재/부재 시 동작, stable mode 버퍼 제거 및 stats 업데이트.
- QueueManager.checkState/checkStore: store 파일 누락, map 닫힘 시 `UnsteadyStateException` 발생.
- ControllerFactory.checkFile: 경로 없음/비정상(빈 값, 쓰기 불가) 검증 실패 케이스.
- ControllerFactory.checkStore: unsaved changes 롤백 성공/실패 시 동작 및 InitializeException 경로.

## 우선순위 P1 (기능 완성도/회귀 방지)
- DataAccess stable mode: 버퍼 우선 반환 + TTL 경과 시 경고 로그만 남기고 반환되는 동작 확인.
- DataAccess.writeBulk/ writeQueueData: bulkCommit true/false, size 기준 commit 동작.
- QueueManager.realignDataKey/realignDataKeyIfNeeded: mismatch 복구 및 1000회 주기 동작.
- BaseController: 모든 wrapper/관리 메서드(`clearData`, `removeData`, `clearBuffer`, `clearCache`, `removeOne` 등) 커버.
- ControllerFactory.createCustomController: readOnly/compress/encrypt 옵션 MVStore 생성 경로 smoke 테스트.

## 우선순위 P2 (경계값/설정/VO)
- FileQueueConfigVo: 경로 파싱(구분자 없음/끝이 구분자) 및 queuePath+queueName 조합.
- BufferEntry.touch: 접근 시간 갱신 및 data 참조 유지.
- StoreInfo.closeAll: 예외 전파 + suppressed 보존 동작.
- StatsTracker/InOutStorage: OUTPUT_INVOKE/BUFFER_OUTPUT_INVOKE 기록 및 overflow 방어 로직.
- QueueSummaryVo.toString 등 단순 VO 동작.

## 클래스별 누락 테스트 목록

### ControllerFactory (src/main/java/com/queue/file/controller/ControllerFactory.java)
- create(queuePath, queueName) 정상 경로.
- createCustomController: readOnly/compress/encrypt 옵션 적용 여부.
- createBulk: bulkSize 0/음수 처리 정책 확인(문서화 필요).
- checkFile: 빈 경로/이름/쓰기 불가 경로 예외.
- checkFile: queue 파일 존재 시 restoreMode 설정.
- checkStore: dirty 상태 롤백 성공/실패 경로.
- checkStore: lastChunkId 임계치 + format < 3일 때 예외.
- getWriteFormatVersion: header format 미존재 시 기본값 3 처리.
- checkStore: MVStoreException/예상치 못한 예외가 InitializeException으로 래핑되는지.

### QueueManager (src/main/java/com/queue/file/controller/QueueManager.java)
- realignDataKey: dataMap/키 리스트 mismatch 복구 + DataSyncInfo/index 업데이트.
- realignDataKeyIfNeeded: 1000회 주기로만 동작.
- checkState: store/map 닫힘 시 예외.
- cleanExpiredBufferEntries: TTL <= 0 no-op, 만료/NULL 엔트리 처리.
- checkStore: store 파일 누락 시 예외.

### DataAccess (src/main/java/com/queue/file/controller/DataAccess.java)
- write: null/빈 문자열 무시.
- writeBulk: null/빈 리스트 무시, FileQueueData 변환 확인.
- writeQueueData: bulkCommit 조건부 commit/rollback 경로.
- read stable mode: buffer 우선 반환 + buffer access time 갱신.
- read: requestCount <= 0, dataMap empty 경로.
- read: stable mode buffer 저장 및 stats 증가.
- readCommit: storeOpenTime null 예외, buffer 없을 때 no-op.
- extractData: dataMap/transactionKeyList 동기화.
- removeOne: buffer 우선 제거, 없으면 dataMap에서 1건 제거.
- getSummaryInfo: cache count 반영.
- cache 관련 메서드: getCache, getCacheList, clearCache, clearCache(key).
- clearData/removeData: transactionKeyList 정리까지 반영.

### BaseController (src/main/java/com/queue/file/controller/BaseController.java)
- tag/producer wrapper 메서드 동작 및 예외 전파.
- read(String), read(String,int) wrapper 경로.
- 관리 메서드 전체 커버(getData/getBuffer/getCache/clear/remove).
- checkState 예외 전파.
- cache store helper(getCacheStore/openCacheMap) 사용.
- close: lock 획득 실패 시 로그/강제 close 경로, closeAll 예외 처리.

### StoreInfo (src/main/java/com/queue/file/vo/StoreInfo.java)
- closeAll: main store/cache store 모두 닫힘, 예외 전파 및 suppressed 처리.
- setStore/setCacheStore: open time 설정.

### StatsTracker / InOutStorage
- OUTPUT_INVOKE/BUFFER_OUTPUT_INVOKE 기록 확인.
- overflow 방어: Long.MAX_VALUE 인접 시 count 리셋 로직.

### Value Objects / Utils
- FileQueueConfigVo 파싱/조합 경계값.
- BufferEntry.touch 동작.
- QueueSummaryVo.toString 형식.

## 테스트 개선(리팩터링) 제안
- 테스트 공통 유틸 추가: temp 디렉터리 생성/삭제, MVStore open/close 헬퍼.
- BaseController/ControllerFactory 테스트의 중복 정리(큐 생성/삭제/커밋).
- stable/bulk 모드 생성 헬퍼로 고정 값 관리.
- 경로/파일 삭제는 공통 메서드에서 일괄 처리(실패 무시 로직 통합).
- 공통 asserts 도입: queue 데이터 수, buffer 존재 여부, cache entry count.
- 테스트 이름 규칙 통일: 동작_조건_기대결과 형태로 가독성 강화.
