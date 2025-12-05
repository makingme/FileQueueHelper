# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

FileQueueHelper는 H2 MVStore 기반의 파일 큐 데이터 파이프라인 라이브러리입니다. Java 8 기반으로 개발되었으며, Maven Central에 배포됩니다.

- **GroupId**: io.github.makingme
- **ArtifactId**: FileQueueHelper
- **Version**: 2.1.0
- **License**: MIT

## 빌드 및 테스트 명령어

### 프로젝트 빌드
```bash
mvn clean compile
```

### 테스트 실행
```bash
# 전체 테스트 실행
mvn test

# 특정 테스트 클래스 실행
mvn test -Dtest=BaseControllerTest

# 특정 테스트 메서드 실행
mvn test -Dtest=BaseControllerTest#testWrite
```

### 패키지 생성
```bash
mvn package
```

### 배포 (Central Publishing)
```bash
# GPG 서명과 함께 Central에 배포
mvn clean deploy
```

### Javadoc 생성
```bash
mvn javadoc:javadoc
```

## 아키텍처 개요

### 핵심 컴포넌트

1. **ControllerFactory** (src/main/java/com/queue/file/controller/ControllerFactory.java)
   - BaseController 인스턴스를 생성하는 팩토리 클래스
   - 다양한 모드 지원: 기본, Stable, Bulk, Custom
   - MVStore 초기화 및 Health Check 수행
   - `checkStore()`: lastChunkId 점검, unsaved changes 처리, write format 버전 확인

2. **BaseController** (src/main/java/com/queue/file/controller/BaseController.java)
   - 메인 API 인터페이스
   - write/writeBulk: 데이터 쓰기
   - read/readCommit: 데이터 읽기 및 커밋
   - AutoCloseable 구현으로 자동 리소스 관리

3. **QueueManager** (src/main/java/com/queue/file/controller/QueueManager.java)
   - 단일 큐 데이터 관리
   - QueueContext를 통한 DataMap, ReadBufferMap, CacheMap 관리
   - 큐 초기화 및 데이터 키 재정렬

4. **DataAccess** (src/main/java/com/queue/file/controller/DataAccess.java)
   - 실제 데이터 읽기/쓰기 로직
   - Lock 메커니즘을 통한 동시성 제어
   - Tag 기반 데이터 분류 및 Executor별 버퍼 관리

5. **StatsTracker** (src/main/java/com/queue/file/controller/StatsTracker.java)
   - 통계 정보 추적 및 관리

### 데이터 구조

파일큐는 다음 구성요소로 이루어집니다 (temp.txt 참조):

- **Wlock 파일**: 쓰기 작업 Lock
- **Rlock 파일**: 읽기 작업 Lock
- **Header 파일**: 쓰기 작업 메타데이터 (파일인덱스, 라인인덱스, offset, length, 상태)
- **Tail 파일**: 읽기 작업 메타데이터 (처리 상태 추적)
- **큐 데이터 파일**: 실제 데이터 저장

읽기와 쓰기가 별도 Lock으로 동시 수행 가능합니다.

### VO (Value Objects)

- **FileQueueConfigVo**: 기본 큐 설정 (경로, 이름, 암호화/압축 모드)
- **FileQueueCustomConfigVo**: 커스텀 설정 (autoCommit, cache, bulk 옵션)
- **FileQueueData**: 큐 데이터 엔티티
- **QueueContext**: 단일 큐의 MVMap 컨텍스트
- **StoreInfo**: MVStore 및 설정 정보 보관

### 예외 계층

- QueueException (기본)
  - InitializeException: 초기화 실패
  - QueueReadException: 읽기 실패
  - QueueWriteException: 쓰기 실패
  - UnsteadyStateException: 불안정 상태

## 개발 패턴

### Controller 생성 패턴

```java
// 기본 모드
BaseController controller = ControllerFactory.create(queuePath);

// Stable 모드 (안정성 우선)
BaseController stableController = ControllerFactory.createStable(queuePath);

// Bulk 모드 (배치 커밋)
BaseController bulkController = ControllerFactory.createBulk(queuePath, 100);

// Custom 모드
FileQueueConfigVo config = new FileQueueConfigVo(queuePath, queueName);
config.setEncryptMode(true);
config.setCustomConfig(customConfigVo);
BaseController customController = ControllerFactory.createCustomController(config);
```

### Write/Read 패턴

```java
// 쓰기
controller.write("data");
controller.write("tag1", "executor1", "data");
controller.writeBulk("tag1", "executor1", dataList);

// 읽기
List<FileQueueData> data = controller.read("executorName", 10);
controller.readCommit("executorName");
```

## 테스트 구조

- **src/test/java/**: JUnit 기반 단위 테스트
  - BaseControllerTest, DataAccessTest 등
- **src/test/com/queue/sample/**: 샘플 및 부하 테스트
  - LoadTest.java: 멀티 프로듀서/컨슈머 부하 테스트 (TPS 측정 포함)
  - ProducerSample.java, ConsumerSample.java: 사용 예제

## 주요 의존성

- **H2 MVStore 2.2.224**: 파일 기반 해시 스토어 (Java 8 호환)
- **SLF4J 1.7.5**: 로깅 API
- **JUnit**: 테스트 프레임워크

## Central 배포 설정

Maven Central 배포를 위해 다음이 자동으로 처리됩니다:
- sources.jar 생성 (maven-source-plugin)
- javadoc.jar 생성 (maven-javadoc-plugin)
- GPG 서명 (maven-gpg-plugin)
- Central Publishing (central-publishing-maven-plugin)
