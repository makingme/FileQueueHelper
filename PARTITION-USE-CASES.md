# 파티션 사용 사례 가이드

## 파티션이란?

파티션은 **하나의 큐를 논리적으로 분리**하여 **서로 다른 종류의 데이터를 독립적으로 처리**할 수 있게 하는 기능입니다.

---

## 실전 사용 사례

### 1. 🏪 전자상거래: 업무 도메인별 분리

#### 문제 상황
```
온라인 쇼핑몰에서 주문, 결제, 배송, 재고 관리를 모두 하나의 큐로 처리하면:
- 결제 지연 시 주문 처리도 지연
- 재고 업데이트가 밀리면 전체 시스템 느려짐
- 중요도가 다른 작업들이 섞여서 우선순위 조절 불가
```

#### 파티션 활용
```java
BaseController queue = ControllerFactory.createWithSeparatePartitionFiles(
    "/data/commerce", "order-queue"
);

// 각 도메인별로 독립 큐
queue.write("order", orderData);       // 주문 접수
queue.write("payment", paymentData);   // 결제 처리
queue.write("shipping", shippingData); // 배송 지시
queue.write("inventory", stockData);   // 재고 업데이트
```

#### 처리 구조
```java
// 주문 처리 워커 (3개 스레드)
for (int i = 0; i < 3; i++) {
    new Thread(() -> {
        while (running) {
            List<FileQueueData> orders = queue.read("order", "order-worker-" + id, 50);
            processOrders(orders);  // 주문만 처리
            queue.readCommit("order-worker-" + id);
        }
    }).start();
}

// 결제 처리 워커 (5개 스레드 - 중요!)
for (int i = 0; i < 5; i++) {
    new Thread(() -> {
        while (running) {
            List<FileQueueData> payments = queue.read("payment", "payment-worker-" + id, 20);
            processPayments(payments);  // 결제만 처리 (독립적!)
            queue.readCommit("payment-worker-" + id);
        }
    }).start();
}
```

#### 장점
- ✅ 결제 지연이 주문 처리에 영향 없음
- ✅ 중요한 업무(결제)에 더 많은 워커 할당 가능
- ✅ 각 도메인별 독립적인 모니터링/로깅
- ✅ v2.1.0: 각 파티션이 독립 파일이라 I/O 경합 없음

---

### 2. 📊 로그 수집 시스템: 로그 레벨별 분리

#### 문제 상황
```
모든 로그를 하나의 큐에 넣으면:
- ERROR 로그 처리가 DEBUG 로그에 밀림
- 디버그 로그가 많을 때 중요한 에러를 놓칠 수 있음
- 로그 레벨별 통계 집계가 어려움
```

#### 파티션 활용
```java
BaseController logQueue = ControllerFactory.createWithSeparatePartitionFiles(
    "/var/logs", "app-logs"
);

// 로그 레벨별 파티션
public void log(String level, String message) {
    switch (level) {
        case "ERROR":
            logQueue.write("error", message);   // 최우선
            break;
        case "WARN":
            logQueue.write("warn", message);
            break;
        case "INFO":
            logQueue.write("info", message);
            break;
        case "DEBUG":
            logQueue.write("debug", message);   // 낮은 우선순위
            break;
    }
}
```

#### 처리 구조
```java
// ERROR 로그는 즉시 처리 (전용 워커)
Thread errorProcessor = new Thread(() -> {
    while (running) {
        List<FileQueueData> errors = logQueue.read("error", "error-handler", 100);
        if (errors != null) {
            sendAlertToSlack(errors);     // 슬랙 알림
            saveToErrorDB(errors);         // 별도 DB 저장
            logQueue.readCommit("error-handler");
        }
        Thread.sleep(100);  // 빠른 폴링
    }
});

// DEBUG 로그는 배치 처리 (낮은 우선순위)
Thread debugProcessor = new Thread(() -> {
    while (running) {
        List<FileQueueData> debugs = logQueue.read("debug", "debug-handler", 1000);
        if (debugs != null) {
            saveToFileStorage(debugs);  // 파일에 저장만
            logQueue.readCommit("debug-handler");
        }
        Thread.sleep(5000);  // 5초마다 배치
    }
});
```

#### 장점
- ✅ 중요한 로그(ERROR)를 우선 처리
- ✅ DEBUG 로그가 많아도 ERROR 처리 지연 없음
- ✅ 로그 레벨별 독립적인 보관 정책 가능
- ✅ 레벨별 통계 집계 용이

---

### 3. 🎮 게임 서버: 이벤트 타입별 분리

#### 문제 상황
```
채팅, 이동, 전투, 아이템 거래를 하나의 큐로 처리하면:
- 전투 이벤트 처리가 채팅 메시지에 밀림
- 실시간성이 중요한 이벤트가 지연될 수 있음
```

#### 파티션 활용
```java
BaseController gameQueue = ControllerFactory.createWithSeparatePartitionFiles(
    "/data/game", "event-queue"
);

// 이벤트 타입별 파티션
gameQueue.write("combat", combatEvent);      // 전투 (실시간)
gameQueue.write("chat", chatMessage);        // 채팅 (실시간)
gameQueue.write("movement", moveEvent);      // 이동 (준실시간)
gameQueue.write("trade", tradeEvent);        // 거래 (배치 가능)
gameQueue.write("quest", questUpdate);       // 퀘스트 (배치 가능)
```

#### 처리 구조
```java
// 전투는 최고 우선순위 (10개 워커)
for (int i = 0; i < 10; i++) {
    executorService.submit(() -> {
        while (running) {
            List<FileQueueData> combats = gameQueue.read("combat", "combat-" + id, 50);
            processCombatEvents(combats);  // 즉시 처리
            gameQueue.readCommit("combat-" + id);
        }
    });
}

// 거래는 낮은 우선순위 (2개 워커)
for (int i = 0; i < 2; i++) {
    executorService.submit(() -> {
        while (running) {
            List<FileQueueData> trades = gameQueue.read("trade", "trade-" + id, 200);
            processTradeEvents(trades);  // 배치 처리
            gameQueue.readCommit("trade-" + id);
            Thread.sleep(1000);
        }
    });
}
```

#### 장점
- ✅ 전투/채팅은 실시간, 거래/퀘스트는 배치 처리 가능
- ✅ 워커 수를 중요도에 따라 차등 배분
- ✅ v2.1.0: 파티션별 독립 파일로 성능 극대화

---

### 4. 🏭 IoT/센서 데이터: 디바이스 타입별 분리

#### 문제 상황
```
공장의 온도 센서, 압력 센서, 진동 센서 데이터를 하나의 큐로 처리하면:
- 각 센서별 처리 로직이 달라서 복잡
- 특정 센서 데이터가 폭증하면 다른 센서 처리 지연
```

#### 파티션 활용
```java
BaseController sensorQueue = ControllerFactory.createWithSeparatePartitionFiles(
    "/data/iot", "sensor-data"
);

// 센서 타입별 파티션
public void onSensorData(String sensorType, String data) {
    switch (sensorType) {
        case "temperature":
            sensorQueue.write("temp", data);
            break;
        case "pressure":
            sensorQueue.write("pressure", data);
            break;
        case "vibration":
            sensorQueue.write("vibration", data);
            break;
        case "humidity":
            sensorQueue.write("humidity", data);
            break;
    }
}
```

#### 처리 구조
```java
// 온도 센서: 1분 평균 계산
Thread tempProcessor = new Thread(() -> {
    while (running) {
        List<FileQueueData> temps = sensorQueue.read("temp", "temp-analyzer", 1000);
        if (temps != null) {
            double avg = calculateAverage(temps);
            if (avg > THRESHOLD) sendAlert("온도 경보!");
            sensorQueue.readCommit("temp-analyzer");
        }
    }
});

// 진동 센서: 실시간 이상 탐지 (머신러닝)
Thread vibrationProcessor = new Thread(() -> {
    while (running) {
        List<FileQueueData> vibes = sensorQueue.read("vibration", "vibe-ml", 100);
        if (vibes != null) {
            boolean anomaly = mlModel.predict(vibes);
            if (anomaly) emergencyStop();
            sensorQueue.readCommit("vibe-ml");
        }
        Thread.sleep(50);  // 빠른 폴링
    }
});
```

#### 장점
- ✅ 센서별 독립적인 처리 로직
- ✅ 중요 센서(진동)는 실시간, 일반 센서는 배치
- ✅ 센서 추가 시 새 파티션만 추가하면 됨
- ✅ 센서별 데이터 보관 정책 차등 적용 가능

---

### 5. 📧 이메일 발송 시스템: 긴급도별 분리

#### 문제 상황
```
회원가입 인증 메일, 마케팅 메일, 비밀번호 재설정을 하나로 처리하면:
- 마케팅 메일이 많을 때 인증 메일 지연
- 사용자 경험 악화
```

#### 파티션 활용
```java
BaseController emailQueue = ControllerFactory.createWithSeparatePartitionFiles(
    "/data/mail", "outbox"
);

// 긴급도별 파티션
public void sendEmail(EmailType type, Email email) {
    switch (type) {
        case CRITICAL:  // 비밀번호 재설정, OTP
            emailQueue.write("critical", email.toJson());
            break;
        case HIGH:      // 회원가입 인증
            emailQueue.write("high", email.toJson());
            break;
        case NORMAL:    // 주문 확인
            emailQueue.write("normal", email.toJson());
            break;
        case LOW:       // 마케팅 메일
            emailQueue.write("marketing", email.toJson());
            break;
    }
}
```

#### 처리 구조
```java
// Critical: 즉시 발송 (5개 워커)
for (int i = 0; i < 5; i++) {
    new Thread(() -> {
        while (running) {
            List<FileQueueData> critical = emailQueue.read("critical", "crit-" + id, 10);
            sendEmailsImmediately(critical);
            emailQueue.readCommit("crit-" + id);
            Thread.sleep(100);
        }
    }).start();
}

// Marketing: 천천히 발송 (1개 워커, 속도 제한)
new Thread(() -> {
    while (running) {
        List<FileQueueData> marketing = emailQueue.read("marketing", "marketing", 100);
        if (marketing != null) {
            for (FileQueueData email : marketing) {
                sendEmail(email);
                Thread.sleep(1000);  // 1초에 1개씩 (속도 제한)
            }
            emailQueue.readCommit("marketing");
        }
    }
}).start();
```

#### 장점
- ✅ 중요한 메일은 즉시 발송
- ✅ 마케팅 메일은 속도 제한 적용
- ✅ 발송 실패 시 파티션별 재시도 정책 적용 가능

---

## 파티션 사용이 적합하지 않은 경우

### ❌ 단일 업무 흐름
```java
// 나쁜 예: 불필요한 파티션
queue.write("step1", data);
queue.write("step2", data);
queue.write("step3", data);

// 좋은 예: 하나의 큐에서 순차 처리
queue.write(data);
```

### ❌ 데이터가 매우 적음
```
하루 100건 미만의 데이터라면 파티션 없이도 충분
```

### ❌ 모든 데이터를 동일하게 처리
```
처리 로직, 우선순위, 워커 수가 모두 같다면 파티션 불필요
```

---

## 파티션 vs 별도 큐

### 파티션 사용 (같은 큐, 논리적 분리)
```
- 같은 도메인, 다른 타입
- 예: 주문큐 안에서 order/payment/shipping
- 하나의 애플리케이션에서 관리
- 설정 공유 (autoCommit, bulkSize 등)
```

### 별도 큐 사용 (물리적 분리)
```
- 완전히 다른 도메인
- 예: order-queue, log-queue, notification-queue
- 다른 애플리케이션/서버에서 관리
- 독립적인 설정
```

---

## v2.1.0 파티션별 파일 분리의 진가

### 기존 (v2.0.0)
```
파티션이 많아도 하나의 파일 I/O 공유
→ 파티션 5개여도 성능은 단일 큐와 비슷
→ 논리적 분리만 가능
```

### v2.1.0
```java
BaseController queue = ControllerFactory.createWithSeparatePartitionFiles(
    "/data/queue", "myqueue"
);

// 각 파티션이 독립 파일!
// myqueue_order.mv
// myqueue_payment.mv
// myqueue_shipping.mv

// → 5개 파티션 = 5배 I/O 성능!
// → 물리적 분리 + 논리적 분리
```

---

## 실전 권장 사항

1. **3개 이상 타입**이 있고, **처리 방식이 다르면** → 파티션 사용
2. **우선순위**가 필요하면 → 파티션 사용
3. **독립적인 스케일링**이 필요하면 → 파티션 사용
4. **v2.1.0 + 멀티 파티션** → 파티션별 파일 모드 사용!

파티션은 "같은 큐 안에서 다른 종류의 데이터를 다르게 처리하고 싶을 때" 사용하는 것입니다.
