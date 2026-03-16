# Repository Guidelines

## Project Structure & Module Organization
- `src/main/java/com/queue/file/controller`: Queue entry points (BaseController, ControllerFactory, QueueManager, StatsTracker) and file-backed coordination logic.
- `src/main/java/com/queue/file/vo` / `exception` / `utils`: Value objects for configs and payloads, checked exceptions, and small helpers.
- Tests live in `src/test/java/com/queue/file/controller/*Test.java` (unit) and `src/test/com/queue/sample` (producer/consumer demos plus `LoadTest`).
- Build output lands in `target/`; docs and release notes sit at repo root (`README.md`, `v2.1.0-USAGE.md`, `v2.1.0-MIGRATION.md`).

## Build, Test, and Development Commands
- `mvn clean verify` - full build plus unit tests; runs signing at `verify`, so use only if you have GPG keys configured.
- `mvn clean package -DskipTests` - build the jar without tests/signing (fast local iteration).
- `mvn test` - run JUnit 4 test suite; prefer before every PR.
- `mvn test-compile exec:java -Dexec.mainClass="com.queue.file.sample.LoadTest"` - optional throughput smoke run; writes to a temp MVStore file.

## Coding Style & Naming Conventions
- Java 8 target; UTF-8 source files; use 4-space indentation and keep lines readable (<120 cols when possible).
- Packages are lower-case; classes in PascalCase; methods/fields in camelCase; constants in UPPER_SNAKE_CASE.
- Favor SLF4J logging over `System.out`; keep queue mutations thread-safe (follow existing `ReentrantReadWriteLock` patterns).
- Avoid unchecked exceptions for queue operations; use the defined `Queue*Exception` types and surface actionable messages.

## Testing Guidelines
- JUnit 4 (`*Test` suffix) is the baseline; mock external resources and use temp files for queue paths to avoid clobbering local data.
- Cover read/write/commit flows and concurrency edge cases (e.g., empty reads, bulk commits, retry behavior).
- Keep load/perf checks out of CI; run `LoadTest` manually when touching performance-sensitive code.

## Commit & Pull Request Guidelines
- Use concise, present-tense subjects; follow repo history by prefixing with version or module when relevant (e.g., `v2.1.0: QueueManager init fix`).
- Include a short problem/solution note, affected modules, and testing done (`mvn test`, manual steps).
- PRs should link issues or describe the scenario, list functional impacts, and attach screenshots/logs when UI or metrics change.
- Avoid committing build artifacts (`target/`) or local credentials; update docs/migration notes when behavior or defaults shift.

## Storage & Configuration Notes
- Queues persist to H2 MVStore files; supply explicit, per-environment paths to avoid collisions and clean up temp files after tests.
- Prefer `ControllerFactory` helpers to build controllers; this wires `QueueManager`, `DataAccess`, and stats tracking consistently.
