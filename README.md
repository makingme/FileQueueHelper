# FileQueueHelper

파일 기반 영속 큐 라이브러리 - H2 MVStore를 사용한 고성능 큐 시스템

## 특징

- ✅ **파일 기반 영속성**: 메모리 큐와 달리 프로세스 재시작 후에도 데이터 보존
- ✅ **고성능**: H2 MVStore 기반으로 빠른 읽기/쓰기 성능
- ✅ **트랜잭션 지원**: Read Buffer를 통한 안전한 2-phase commit
- ✅ **동시성 제어**: ReentrantReadWriteLock 기반 스레드 안전성
- ✅ **Bulk 작업**: 대량 데이터 일괄 처리 지원
- ✅ **통계 추적**: 입출력 건수 및 TPS 모니터링

## 버전

- **Latest**: v2.1.0 (단일 큐 방식)
- **Previous**: v2.0.0 (멀티 파티션 방식)

> ⚠️ v2.1.0은 파티션 개념을 완전히 제거한 Breaking Change 버전입니다.
> v2.0.0에서 마이그레이션하시려면 [v2.1.0 마이그레이션 가이드](v2.1.0-MIGRATION.md)를 참조하세요.

## 설치

### Maven
```xml
<dependency>
    <groupId>com.queue.file</groupId>
    <artifactId>FileQueueHelper</artifactId>
    <version>2.1.0</version>
</dependency>
```

### Gradle
```gradle
implementation 'com.queue.file:FileQueueHelper:2.1.0'
```

## 빠른 시작

### 1. 기본 사용법

```java
import com.queue.file.controller.BaseController;
import com.queue.file.controller.ControllerFactory;
import com.queue.file.vo.FileQueueData;

// 큐 생성
BaseController queue = ControllerFactory.create("myqueue.mv");

// 데이터 쓰기
queue.write("Hello World");

// 데이터 읽기
FileQueueData data = queue.read();
System.out.println(data.getData());  // "Hello World"

// 읽기 커밋 (버퍼에서 제거)
queue.readCommit(Thread.currentThread().getName());

// 큐 닫기
queue.close();
```

### 2. Bulk 작업

```java
// 대량 데이터 쓰기
List<String> dataList = Arrays.asList("data1", "data2", "data3", "data4", "data5");
queue.writeBulk(dataList);

// 대량 데이터 읽기 (최대 10개)
List<FileQueueData> results = queue.read("myConsumer", 10);
for (FileQueueData item : results) {
    System.out.println(item.getData());
}

// 읽기 커밋
queue.readCommit("myConsumer");
```

### 3. Tag 및 Consumer/Producer 사용

```java
// Tag와 Producer를 지정한 쓰기
queue.write("importantTag", "producer1", "critical data");

// Consumer별 읽기
FileQueueData data1 = queue.read("consumer1");
FileQueueData data2 = queue.read("consumer2");

// Consumer별 커밋
queue.readCommit("consumer1");
queue.readCommit("consumer2");
```

### 4. 큐 상태 조회

```java
// 요약 정보 조회
QueueSummaryVo summary = queue.getSummaryInfo();
System.out.println("데이터 건수: " + summary.getDataCount());
System.out.println("버퍼 건수: " + summary.getBufferCount());
System.out.println("캐시 건수: " + summary.getCacheCount());

// 전체 데이터 목록 조회
List<FileQueueData> dataList = queue.getDataList();

// 버퍼 목록 조회 (Consumer별)
Map<String, List<FileQueueData>> bufferList = queue.getBufferList();
```

### 5. 관리 작업

```java
// 특정 데이터 조회
FileQueueData data = queue.getData(12345L);  // transactionKey로 조회

// 특정 데이터 삭제
queue.removeData(12345L);

// 전체 데이터 삭제
queue.clearData();

// 버퍼 삭제
queue.clearBuffer("consumer1");  // 특정 consumer 버퍼만
queue.clearBuffer();              // 전체 버퍼

// 캐시 관리
queue.clearCache("cacheKey");
queue.clearCache();
```

## 고급 사용법

### 멀티 스레드 환경

```java
BaseController queue = ControllerFactory.create("queue.mv");

// Producer 스레드
CompletableFuture.runAsync(() -> {
    for (int i = 0; i < 1000; i++) {
        queue.write("data-" + i);
    }
});

// Consumer 스레드
CompletableFuture.runAsync(() -> {
    while (true) {
        FileQueueData data = queue.read("consumer1");
        if (data != null) {
            // 데이터 처리
            processData(data);
            queue.readCommit("consumer1");
        } else {
            Thread.sleep(100);
        }
    }
});
```

### 커스텀 설정

```java
import com.queue.file.vo.FileQueueConfigVo;
import com.queue.file.vo.FileQueueCustomConfigVo;

// 커스텀 설정
FileQueueConfigVo config = new FileQueueConfigVo("/data/queues", "myqueue.mv");
config.setQueueName("MyCustomQueue");

FileQueueCustomConfigVo customConfig = new FileQueueCustomConfigVo();
customConfig.setBulkCommit(true);  // Bulk 작업 시 자동 커밋

BaseController queue = ControllerFactory.create(config, customConfig);
```

### AutoCloseable 지원

```java
// try-with-resources 사용
try (BaseController queue = ControllerFactory.create("queue.mv")) {
    queue.write("data");
    FileQueueData data = queue.read();
    queue.readCommit(Thread.currentThread().getName());
} // 자동으로 close() 호출됨
```

## 아키텍처

### 데이터 흐름

```
[Write] → DataMap → [Read] → ReadBuffer → [ReadCommit] → 삭제
                          ↓
                      CacheMap (선택적)
```

1. **Write**: 데이터가 DataMap에 저장됨
2. **Read**: DataMap에서 ReadBuffer로 이동 (임시 버퍼)
3. **ReadCommit**: ReadBuffer에서 완전히 제거 (처리 완료)

### 핵심 컴포넌트

- **BaseController**: 사용자 API 제공
- **DataAccess**: 실제 큐 데이터 조작
- **QueueManager**: 단일 큐 컨텍스트 관리
- **StatsTracker**: 입출력 통계 추적
- **StoreInfo**: H2 MVStore 관리

## 성능

### v2.1.0 개선사항

- DataAccess 코드: 664줄 → 477줄 (28% 감소)
- 파티션 그룹핑 로직 제거로 write 성능 향상
- 2-tier 락 → 단일 write lock으로 read 성능 향상

### LoadTest 예제

프로젝트에 포함된 LoadTest로 성능을 확인할 수 있습니다:

```java
// src/test/com/queue/sample/LoadTest.java
mvn test-compile exec:java -Dexec.mainClass="com.queue.file.sample.LoadTest"
```

## v2.0.0에서 마이그레이션

v2.0.0에서 v2.1.0으로 업그레이드하는 경우:

1. **파티션 파라미터 제거**: 모든 API에서 `partition` 파라미터 삭제
2. **API 변경 대응**: [마이그레이션 가이드](v2.1.0-MIGRATION.md) 참조
3. **테스트**: 기존 테스트 케이스 수정 및 검증

## API 문서

### BaseController 주요 메서드

| 메서드 | 설명 |
|--------|------|
| `write(String data)` | 데이터 쓰기 |
| `write(String tag, String producerId, String data)` | Tag/Producer 지정 쓰기 |
| `writeBulk(List<String> dataList)` | 대량 데이터 쓰기 |
| `read()` | 단일 데이터 읽기 |
| `read(String consumerId)` | Consumer 지정 읽기 |
| `read(String consumerId, int count)` | 대량 데이터 읽기 |
| `readCommit(String consumerId)` | 읽기 커밋 |
| `getSummaryInfo()` | 큐 요약 정보 조회 |
| `getDataList()` | 전체 데이터 목록 조회 |
| `clearData()` | 전체 데이터 삭제 |

## 라이선스

이 프로젝트의 라이선스는 프로젝트 소유자에게 문의하세요.

## 기여

버그 리포트 및 기능 제안은 이슈로 등록해주세요.

## 변경 이력

### v2.1.0 (2025-11-28)
- **Breaking Change**: 파티션 개념 완전 제거
- 단일 큐 방식으로 전환
- API 단순화 (partition 파라미터 제거)
- 코드 리팩토링으로 28% 코드 감소
- 성능 개선 (락 단순화)

### v2.1.0
- 캐시 Store 추가
- LoadTest TPS 정보 출력

### v2.0.0
- 멀티 파티션 지원
- Bulk 작업 성능 개선

## 문의

프로젝트 관련 문의사항은 이슈를 통해 남겨주세요.
