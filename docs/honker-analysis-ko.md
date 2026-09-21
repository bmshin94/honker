# honker 전수조사 분석 정리 (한국어)

> 작성일: 2026-09-21
> 대상 커밋: `8233aa5` / 브랜치 `claude/sweet-edison-1ot1bh`
> 정리: Claude Code 세션 대화 내용 요약

## 관련 깃허브 주소

| 구분 | 주소 |
| --- | --- |
| 원본 저장소 (upstream) | https://github.com/russellromney/honker |
| 이 저장소 (fork) | https://github.com/bmshin94/honker |
| 공식 문서 사이트 | https://honker.dev |
| ORM 가이드 | https://honker.dev/guides/orm/ |
| Simon Willison 소개글 | https://simonwillison.net/2026/Apr/24/honker/ |
| PyPI (Python) | https://pypi.org/project/honker/ |
| npm (Node) | https://www.npmjs.com/package/@russellthehippo/honker-node |

참고로 비교 대상으로 언급한 프로젝트들:

- pg-boss — https://github.com/timgit/pg-boss
- Oban — https://hexdocs.pm/oban/
- Huey — https://github.com/coleifer/huey
- Brandur, "Transactionally Staged Job Drains in Postgres" — https://brandur.org/job-drain

---

## 1. honker는 무엇인가

**SQLite 파일 하나로 Redis + Celery(브로커 + 작업 큐)를 대체하는 엔진.**

정식으로는 `SQLite 로더블 확장(loadable extension) + 11개 언어 바인딩`이며,
PostgreSQL의 `NOTIFY`/`LISTEN` 의미론을 SQLite에 이식하고 그 위에
영속 pub/sub, 태스크 큐, 이벤트 스트림, 크론 스케줄러를 올렸다.
별도 브로커나 데몬 프로세스가 필요 없다.

- 버전: `0.5.0` (**Alpha** — 저장소 README가 "better than experimental, not beta-quality yet"이라고 명시)
- 라이선스: **Apache-2.0 OR MIT** (듀얼 라이선스)
- 저장소 규모: 450개 파일 / 약 4.5MB
- 언어 분포: Java 75, Python 68, Markdown 38, C# 23, JS 22, Rust 21, Ruby 20, Go 12, Elixir 24(ex/exs), C++ 4

### 해결하는 문제

기존 구조(앱 → SQLite + 별도 Redis)는 세 가지 고질병이 있다.

1. **이중 쓰기(dual-write)** — 비즈니스 행은 커밋됐는데 큐 삽입이 실패하면 작업이 유실된다.
2. **운영 부담** — 브로커 서버, 백업, 모니터링, 장애 지점이 하나 더 늘어난다.
3. **폴링 낭비** — 큐 테이블을 주기적으로 `SELECT`하며 CPU/디스크를 소모한다.

honker의 답: **큐를 같은 SQLite 파일 안에 넣는다.**

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
# 둘이 같은 트랜잭션에서 커밋. 롤백하면 둘 다 사라진다.
```

이것이 **transactional outbox 패턴**이며, Simon Willison이 자기 블로그에서
"SQLite로 구현한 transactional outbox"로 소개하면서 화제가 되었다.

---

## 2. 동작 원리

SQLite에는 서버 측 푸시 채널이 없다. honker는 다음 방식으로 푸시에 준하는 동작을 만든다.

```
1. PRAGMA data_version 을 1ms 간격으로 읽는다  (한 자릿수 마이크로초, 거의 공짜)
2. 카운터 값이 바뀌면 = 누군가 커밋했다는 뜻
3. 그때만 인덱스를 태운 SELECT 한 번으로 실제 일감을 확인한다
```

- 크로스 프로세스 전달 지연: **한 자릿수 밀리초**
- `Database` 하나당 watcher **1개**를 공유 (구독자 100명이어도 watcher는 하나)
- 유휴 상태에서는 큐/알림 SELECT **0회**
- 과잉 트리거(overtriggering)는 **의도적** — "인덱스 SELECT 한 번은 싸지만, 놓친 wake는 정합성 버그다"

안정 백엔드는 `PRAGMA data_version`이고, 실험적 백엔드로 `kernel`(파일시스템 이벤트),
`shm`(WAL 공유 메모리 mmap 읽기)이 소스 빌드에서만 제공된다.
실험 백엔드를 명시 요청했는데 지원이 없으면 **조용히 폴링으로 폴백하지 않고 에러**를 낸다.

---

## 3. 저장소 구조

```
honker/
├── honker-core/          Rust 엔진 (핵심, 총 8,368줄)
│   ├── lib.rs            3,982줄 — 스키마, watcher, 트랜잭션
│   ├── honker_ops.rs     3,312줄 — 큐/스트림/락/결과 로직
│   ├── cron.rs             473줄 — cron 파서 (5필드/6필드/@every)
│   ├── kernel_watcher.rs   601줄 — [실험] OS 파일 이벤트
│   └── shm_watcher.rs      465줄 — [실험] WAL SHM mmap
│
├── honker-extension/     SQLite 로더블 확장 (390줄)
│
├── packages/             11개 언어 바인딩
│   ├── honker/           Python (레퍼런스 구현, 기능 최다)
│   ├── honker-node/      Node.js
│   ├── honker-bun/       Bun
│   ├── honker-rs/        Rust 래퍼
│   ├── honker-go/        Go
│   ├── honker-ruby/      Ruby
│   ├── honker-dotnet/    .NET / C#
│   ├── honker-ex/        Elixir
│   ├── honker-cpp/       C++
│   ├── honker-jvm/       Java
│   └── honker-kotlin/    Kotlin
│
├── bench/                벤치마크 5종
├── .github/workflows/    CI 13개 (언어별 릴리스 + zizmor 보안감사 + scary-nightly)
├── README.md   BINDINGS.md   ROADMAP.md   CHANGELOG.md   CONTRIBUTING.md
```

핵심은 **로직이 Rust 코어 한 곳에만 있고, 모든 언어가 같은 확장과 같은 테이블 스키마를 공유**한다는 점이다.
그래서 Node로 enqueue한 작업을 Python 워커가 claim하고 Go가 결과를 읽는 조합이 가능하다.

### 주요 테이블

| 테이블 | 용도 |
| --- | --- |
| `_honker_live` | 큐의 대기/처리중 작업 |
| `_honker_dead` | 재시도 소진된 데드레터 |
| `_honker_notifications` | pub/sub 알림 |
| `_honker_stream` | 이벤트 스트림 |
| `_honker_stream_consumers` | 소비자별 오프셋 |
| `_honker_scheduler_tasks` | 크론/주기 태스크 |
| `_honker_locks` | 이름 붙은 락 |
| `_honker_rate_limits` | 레이트 리밋 윈도우 |
| `_honker_results` | 작업 결과 (TTL 포함) |

클레임은 부분 인덱스를 태운 `UPDATE ... RETURNING` 한 번,
ack은 `DELETE` 한 번이다. 인덱스는
`(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')`.

---

## 4. 제공 기능

| 기능 | 설명 | SQL 함수 예시 |
| --- | --- | --- |
| Pub/Sub | 프로세스 간 실시간 알림(휘발성) | `notify('orders', '{"id":42}')` |
| 작업 큐 | 재시도, 지연실행, 우선순위, 가시성 타임아웃, 데드레터 | `honker_claim_batch('emails','w1',32,300)` |
| 스트림 | 소비자별 오프셋 (Kafka 축소판) | `honker_stream_publish('orders','k','{}')` |
| 스케줄러 | cron 5/6필드 + `@every 5s` | `honker_scheduler_register(...)` |
| 분산 락 | TTL 기반 named lock | `honker_lock_acquire('backup','me',60)` |
| 레이트 리밋 | 고정 윈도우 제한 | `honker_rate_limit_try('api',10,60)` |
| 결과 저장 | 작업 리턴값 + TTL | `honker_result_save(42,'{"ok":true}',3600)` |

Python에는 Celery/Huey 스타일 데코레이터(`@task`, `@periodic_task`)와
워커 CLI(`python -m honker worker myapp.tasks:db`)가 있다.
JVM/Kotlin에는 `TaskRegistry` / `runTasks`가 있고, 나머지 바인딩은 큐·결과 프리미티브만 제공한다.

스케줄러는 내부적으로 named lock으로 리더를 선출하고 하트비트로 리더십을 갱신한다.

### 일부러 넣지 않은 것

- 워크플로 DAG, 태스크 체인/그룹/코드
- 멀티라이터 복제
- 머신 간 분산 락

honker는 **단일 머신 / 파일 기반** 전용이다. NFS로 두 서버가 같은 `.db`를 쓰는 것은 지원 구성이 아니다.

---

## 5. 언제 쓰고 언제 쓰면 안 되나

| 상황 | 판단 |
| --- | --- |
| 개인/사이드 프로젝트 | 적합 |
| 서버 1대짜리 스타트업 MVP | 적합 |
| 초당 수천 건 이하 처리량 | 적합 |
| 데스크톱 앱 / CLI 백그라운드 작업 | 적합 |
| IoT / 엣지 디바이스 | 적합 |
| 서버 여러 대로 수평 확장 | **부적합** |
| NFS 등 공유 스토리지 | **부적합 (데이터 손상 위험)** |
| 초당 수만 건 이상 | 부적합 (Kafka/Redis 영역) |
| 복잡한 워크플로 DAG | 기능 없음 |
| 미션 크리티컬 프로덕션 | 주의 — 아직 Alpha |

---

## 6. 설치 및 사용법

### 언어별 설치

| 언어 | 명령어 | 확장 번들 |
| --- | --- | --- |
| Python | `pip install honker` | 포함 (wheel) |
| Node.js | `npm install @russellthehippo/honker-node` | 포함 (`honker-ext-*`) |
| Bun | `bun add @russellthehippo/honker-bun` | 포함 |
| Ruby | `gem install honker` | 포함 (플랫폼 gem) |
| .NET / C# | `dotnet add package Honker` | 포함 (NuGet 네이티브 에셋) |
| Rust | `cargo add honker` | crate 의존성 |
| Java / JVM | `dev.honker:honker` | 포함 (jar 리소스) |
| Kotlin | `dev.honker:honker-kotlin` | JVM 상속 |
| Elixir | Hex `honker` | **미포함 — 릴리스에서 다운로드** |
| Go | in-tree `packages/honker-go` | **미포함 — 릴리스에서 다운로드** |
| C++ | in-tree `packages/honker-cpp` | 정적 링크 |

### Python 최소 예제

```python
import honker, asyncio

db = honker.open("app.db")       # 테이블 자동 생성(bootstrap)
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id, amount) VALUES (?, ?)", [42, 19.99])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)

async def worker():
    async for job in emails.claim("worker-1"):
        send_email(job.payload)
        job.ack()            # 완료 → 행 삭제
        # job.retry(60)      # 60초 뒤 재시도
        # job.fail("에러")   # 데드레터로
        # job.heartbeat()    # 오래 걸리면 클레임 연장

asyncio.run(worker())
```

### 태스크 데코레이터 + 워커 CLI

```python
# myapp/tasks.py
db = honker.open("app.db")
q = db.queue("default")

@q.task(retries=3, retry_delay_s=10, timeout_s=30, store_result=True)
def resize_image(path):
    return do_resize(path)

@db.periodic_task("0 3 * * *", queue="backups")
def nightly():
    backup()
```

```bash
python -m honker worker myapp.tasks:db --queue=default --concurrency=4
python -m honker worker myapp.tasks:db --list
```

### ORM 연결에 확장 로드 (가장 중요)

enqueue를 애플리케이션 트랜잭션 **밖에서** 하면 원자성이 사라진다.
따라서 이미 갖고 있는 커넥션에 확장을 올려야 한다.

```python
import sqlite3, honker
conn = sqlite3.connect("app.db")
honker.load_extension(conn)
conn.execute("SELECT honker_bootstrap()")
```

```js
const Database = require("better-sqlite3");
const { extensionPath } = require("@russellthehippo/honker-node/extension");
const db = new Database("app.db");
db.loadExtension(extensionPath());
db.prepare("SELECT honker_bootstrap()").run();
```

SQLAlchemy, SQLModel, Django, Drizzle, Kysely, sqlx, GORM, ActiveRecord,
Ecto, Hibernate, jOOQ, MyBatis, Exposed에서 동일한 방식이 통한다.

### 확장 경로 해석 규칙 (전 바인딩 공통 계약)

```
명시적 인자 → HONKER_EXTENSION_PATH → 번들 사본 → 에러(탐색한 경로 전부 출력)
```

- `HONKER_EXTENSION_PATH`가 설정됐는데 파일이 없으면 **무조건 에러**. 번들로 조용히 폴백하지 않는다.
- 진입점은 항상 `sqlite3_honkerext_init`.
- 파일명이 load-bearing이다. 반드시 `libhonker_ext.{so,dylib}` / `honker_ext.dll`.
  `libhonker_ext-v2.dylib` 같은 이름은 SQLite가 `sqlite3_honkerextv2_init`을 찾다 실패한다.

### 알려진 삽질 포인트

- `:memory:` DB는 사용 불가. 반드시 파일 기반 SQLite.
- Python 빌드에 확장 로딩이 비활성화돼 있으면 `ExtensionLoadingUnsupported`가 난다.
- 정수 인자는 SQLite C API 방식대로 처리된다. `REAL`이라도 정수값이면 변환되지만
  `2.7` 같은 비정수/NaN/무한대/64비트 범위 밖 값은 **에러**다.
  (better-sqlite3가 모든 JS number를 `REAL`로 바인딩하기 때문에 생긴 명시적 보장)

---

## 7. 플러그인인가, 스킬인가, MCP인가

셋 다 아니다. Claude Code와는 무관한 **일반 소프트웨어 라이브러리(미들웨어)** 다.

| 구분 | 정체 | 대상 |
| --- | --- | --- |
| Claude Plugin | Claude Code 확장 팩 | AI 에이전트 |
| Claude Skill | `SKILL.md` 지침 묶음 | AI 에이전트 |
| MCP | AI ↔ 외부 도구 연결 프로토콜 | AI 에이전트 |
| **honker** | **SQLite 확장 + 언어 바인딩** | **일반 프로그램** |

정확히는 3중 정체성을 가진다.

1. SQLite loadable extension (`libhonker_ext.so`, C ABI 공유 라이브러리)
2. Rust 라이브러리 crate (`honker-core`)
3. 11개 언어 패키지 (pip / npm / gem / nuget / maven / hex / cargo)

다만 honker를 **재료로 써서** MCP 서버를 만드는 것은 가능하고, 실제로 좋은 활용처다.

---

## 8. API 토큰이 필요한가

**필요 없다.** 저장소 전체에 인증/토큰/API 키 관련 코드가 없다.

- 네트워크를 쓰지 않는다. 로컬 `.db` 파일만 읽고 쓴다.
- 서버, 브로커, 클라우드 백엔드가 없으므로 인증 대상 자체가 없다.
- 회원가입, 요금제, 사용량 제한이 없다. 오프라인 완전 동작.
- 필요한 것: SQLite 3.9+ 와 디스크 쓰기 권한.

예외적으로 Go / Elixir / C++ 처럼 확장을 GitHub Release에서 받아야 하는 바인딩이 있는데,
공개 릴리스라 토큰 없이 받을 수 있다. 사내 CI에서 rate limit이 걸릴 때만 GitHub 토큰이 유용하다.

라이선스가 Apache-2.0 OR MIT이므로 **상업적 이용, 수정, 재배포, 유료 판매가 모두 허용**된다.

---

## 9. 왜 깃허브에서 주목받았나

1. **정확한 페인포인트** — SQLite가 주 데이터스토어인 프로젝트는 결국 큐가 필요해지는데,
   지금까지의 답은 "Redis를 붙여라" 뿐이었다. honker는 그 조언을 지운다.
2. **Simon Willison 효과** — Django 공동창시자이자 Datasette 저자가 블로그에서 소개했고
   ("The design of this looks very solid"), 거기서 daily.dev, byteiota 등으로 연쇄 확산됐다.
3. **11개 언어 바인딩의 임팩트** — 개인 프로젝트로는 이례적인 범위.
4. **엔지니어링 퀄리티와 문서의 정직함** — 이게 가장 큰 요인이다.
   - CI 13개 (언어별 릴리스 자동화 + `zizmor` 워크플로 보안 감사 + `scary-nightly` soak)
   - `BINDINGS.md`에 **"Not Proven Yet"** 섹션을 따로 둠
   - Node 0.4.6 스트림 체크포인트 키 뒤바뀜 버그를 ROADMAP에 공개하고 마이그레이션 전략까지 문서화
   - `Phase Kernel-JVM`에 "리눅스에서 깨짐, CI에서 이름으로 제외 중"을 그대로 적어둠
   - 설계 의도를 설명함 ("과잉 트리거는 의도적")
   - 안 넣을 기능을 명시 (DAG, 체인, 복제)
   - 경쟁작 인정 — pg-boss와 Oban을 "우리가 쫓는 golden standard"라고 씀
5. **README의 이 문장** — "If you already run Postgres, use the Postgres tools, as they are excellent."
   자기 프로젝트를 안 써도 된다고 말하는 README가 신뢰를 만든다.

(정확한 스타 수는 이 세션에서 GitHub API 접근이 차단되어 확인하지 못했다.)

---

## 10. 로컬 에이전트 구축에 도움이 되는가

매우 도움이 된다. 로컬 AI 에이전트에 필요한 인프라가 거의 그대로 매핑된다.

| 에이전트에 필요한 것 | honker 기능 |
| --- | --- |
| LLM 호출 백그라운드 처리 | 큐 + 워커 |
| API 실패 자동 재시도 | `retries`, `retry_delay_s`, 지수 백오프 |
| 반복 실패 작업 격리 | 데드레터 `_honker_dead` |
| 에이전트가 죽어도 작업 보존 | SQLite 영속성 + 가시성 타임아웃 |
| 중복 실행 방지 | named lock |
| LLM API 호출 제한 | rate limit |
| "매일 아침 브리핑" 같은 정기 실행 | cron 스케줄러 |
| 실행 이력 재생 | 스트림 + 오프셋 |
| UI 실시간 진행률 | notify / listen (1ms) |
| 결과 캐싱 | result 저장 + TTL |
| 다언어 조합 | 크로스 언어 바인딩 |

결정적인 장점 세 가지:

1. **에이전트 상태와 작업 큐가 같은 파일에 있다.** 메모리 저장이 실패하면 다음 단계도 돌지 않는다.
2. **완전 오프라인 / 프라이버시.** 로컬 LLM(Ollama, llama.cpp)과 조합하면 네트워크 0.
3. **배포가 단순하다.** Electron/Tauri 앱에 넣으면 사용자는 설치만 하면 된다.

권장 아키텍처:

```
┌──────────────┐   enqueue    ┌──────────────┐
│  UI (React)  │ ───────────> │              │
│              │ <─────────── │   agent.db   │   honker (SQLite 파일 1개)
└──────────────┘   listen     │              │
                              │ _honker_live │   큐
┌──────────────┐   claim      │ _honker_*    │   스트림/락/스케줄러
│ Agent Worker │ <──────────> │ agent_memory │   내 테이블
│ (Python+LLM) │              └──────────────┘
└──────────────┘
       │
       └──> Ollama / Claude API / 도구 실행
```

주의: Alpha 단계이고, 단일 머신 전용이며, 워크플로 DAG가 없어 다단계 체인은
각 스텝 끝에서 다음 스텝을 enqueue하는 식으로 직접 구현해야 한다.

---

## 11. React / PHP로 만들 수 있는가

### React

브라우저에서는 **직접 불가능**하다. 브라우저는 로컬 `.db` 파일을 쓸 수 없고
`.so`/`.dll` 네이티브 확장을 로드할 수 없다. 대신 아래 조합이 가능하다.

| 방법 | 구성 | 평가 |
| --- | --- | --- |
| React + Node/Python 백엔드 | `React --fetch/WS--> Express + honker-node --> app.db` | 가장 현실적 |
| Electron / Tauri | `React(렌더러) --IPC--> 메인 프로세스 + honker` | 로컬 에이전트에 최적 |
| Next.js 서버 액션 / API Routes | `'use server'` 안에서 honker-node 호출 | 간편 |
| 브라우저 wasm SQLite(OPFS) + honker | — | **불가능** (wasm 빌드 없음, 프로세스 간 감지가 무의미) |

`db.listen()`을 SSE나 WebSocket에 연결하면 React UI에 진행률을 실시간 표시할 수 있다.

```js
for await (const msg of db.listen("job-progress")) {
  res.write(`data: ${JSON.stringify(msg.payload)}\n\n`);
}
```

### PHP

저장소 전체에 PHP 흔적이 하나도 없다. 공식 바인딩이 **없다.**
하지만 PHP의 `SQLite3` 클래스에 `loadExtension()`이 있고 honker의 기능은
전부 SQL 함수로 노출돼 있으므로, 바인딩 없이도 SQL만으로 사용 가능하다.

```php
<?php
$db = new SQLite3('app.db');
$db->loadExtension('libhonker_ext.so');
$db->exec("SELECT honker_bootstrap()");

$db->exec('BEGIN');
$db->exec("INSERT INTO orders (user_id) VALUES (42)");
$stmt = $db->prepare("SELECT honker_enqueue('emails', :p, NULL, NULL, 0, 3, NULL)");
$stmt->bindValue(':p', json_encode(['to' => 'alice@example.com']));
$stmt->execute();
$db->exec('COMMIT');

$jobs = json_decode($db->querySingle("SELECT honker_claim_batch('emails','w1',32,300)"), true);
foreach ($jobs as $job) {
    sendEmail(json_decode($job['payload'], true));
}
$db->exec("SELECT honker_ack_batch('" . json_encode(array_column($jobs, 'id')) . "', 'w1')");
```

PHP에서의 제약:

- `php.ini`의 `sqlite3.extension_dir` 설정이 필요하다(보안상 기본 비활성인 환경이 많다).
- PHP는 요청마다 프로세스가 종료되므로 `listen()` 같은 장시간 대기에 부적합하다.
  워커는 CLI 모드(`php worker.php`)로 별도 실행해야 한다.
- Rust watcher의 혜택을 받지 못하므로 `data_version` 폴링 루프를 직접 관리해야 한다.

바꿔 말하면 **PHP / Laravel 바인딩은 아직 비어 있는 자리**이고, 선점 기회다.

---

## 12. 수익화 아이디어

전제: 라이선스가 Apache-2.0 OR MIT이므로 상업적 이용·수정·재판매가 전부 허용된다.
honker 자체는 무료이므로, **honker 위에 무엇을 얹느냐**가 수익 지점이다.

### Tier 1 — 난이도 낮고 현실적

**1) Honker Studio — 시각화 대시보드**

honker에는 관리 도구가 전혀 없어 `sqlite3`로 SQL을 직접 쳐야 한다.

- 실시간 대시보드 (큐별 pending/processing, `listen` 활용)
- 데드레터 관리 (실패 목록 + 에러 로그 + 원클릭 재시도)
- 지연 작업 모니터 (`claimed_at` 활용)
- 스케줄러 관리 (등록/일시정지/다음 실행시각)
- 처리량 그래프, 락/레이트리밋 현황, Slack·Discord 알림

스택: React + Vite + Node/Python, Electron 패키징.
가격: Free / Pro $15월 또는 $99 평생 / Team $49월.
근거: Flower, Bull Board, **Sidekiq Pro**가 같은 모델로 성공했다.
개발 기간 2~3주, 난이도 하, 현실성 최상.

**2) Laravel / PHP 큐 드라이버**

```php
'connections' => [
    'honker' => [
        'driver'   => 'honker',
        'database' => database_path('app.sqlite'),
        'queue'    => 'default',
    ],
],
```

```php
DB::transaction(function () {
    $order = Order::create([...]);
    SendReceipt::dispatch($order);   // 같은 트랜잭션에서 커밋
});
```

Laravel 기본 `database` 드라이버는 폴링이라 느리고, 소규모 프로젝트에 Redis 비용은 부담이다.
경쟁자가 없어 선점 가능하다.
코어는 오픈소스 무료로 풀고 Nova 패널($29 일회성), Pro 버전($79/년), 컨설팅으로 수익화.
본체 저장소에 PR을 보내면 공식 컨트리뷰터 이력이 남는다.
개발 기간 3~4주, 난이도 중, 현실성 높음.

**3) Zero-Infra Starter Kit 템플릿 판매**

Next.js 15 + TypeScript + Tailwind + honker 큐/스케줄러/실시간 알림 +
Stripe 결제(멱등성 보장) + 관리자 대시보드 + Docker 단일 컨테이너 배포.
셀링 포인트는 "인프라 비용 월 $0, VPS 한 대면 끝".
Gumroad/LemonSqueezy에서 $79(개인) / $249(팀).
근거: ShipFast($199)가 1년에 $100만 이상을 벌었고, Supastarter·Makerkit도 $299~$999다.
개발 기간 4~6주, 난이도 하, 현실성 높음.

### Tier 2 — 중간 난이도, 큰 잠재력

**4) AI 에이전트 런타임 (LocalAgent Runtime)**

"로컬에서 도는 AI 에이전트를 위한 Zero-Infra 런타임".
10장에서 정리한 매핑(큐, 재시도, 데드레터, 락, 레이트리밋, 스케줄러, 스트림, 결과 캐시)을
하나의 런타임으로 제품화한다.

수익 모델: 오픈소스 코어 + 유료 클라우드 싱크($19/월),
에이전트 템플릿 마켓플레이스 수수료 20~30%,
엔터프라이즈 온프레미스 연 $5,000~ (금융·의료의 "데이터가 회사 밖으로 나가지 않음" 수요).
개발 기간 2~3개월, 난이도 상, 잠재력 매우 높음.

**5) Honker Cloud — 관리형 백업/관측 서비스**

자동 백업(Litestream + S3/R2), 원격 메트릭 수집, 이상 감지 알림,
감사 로그 장기 보관, 여러 서버 DB 통합 뷰. DB당 $9/월, 팀 $49/월.
주의: honker의 "서버 필요 없음" 철학과 충돌하므로 반드시 선택적(opt-in)으로 포지셔닝해야 한다.

**6) SaaS 제품의 "재료"로 써서 원가 절감**

| 제품 | honker 활용 | 원가 우위 |
| --- | --- | --- |
| 셀프호스팅 웹훅 릴레이 | 재시도 + 데드레터 | 인프라 0 |
| 크론잡 모니터링 | 스케줄러 + 알림 | Redis 비용 제거 |
| 이메일/SMS 발송 큐 | outbox 패턴 | 발송 유실 방지 |
| 스크래핑 SaaS | 큐 + 레이트리밋 | 워커당 비용 절감 |
| 영상/이미지 변환 | 타임아웃 + 하트비트 | 브로커 비용 제거 |

경쟁사가 Redis + Postgres + 브로커로 월 $200을 쓸 때 VPS 한 대로 같은 일을 한다.

### Tier 3 — 서비스/콘텐츠형 (초기 자본 0)

**7) 교육 콘텐츠** — 유튜브 "Redis 없이 SaaS 만들기" 시리즈, 전자책($29),
인프런/Udemy 강의($79~149, 특히 "Rust FFI로 다국어 바인딩 만들기"는 희소성이 높다), 기술 블로그 → 뉴스레터.
난이도 최하, 오늘 당장 시작 가능.

**8) 컨설팅 & 마이그레이션 서비스** — "Redis/Celery → honker 이전" 컨설팅(건당 300~1,000만원),
기업 온프레미스 구축 + 교육(프로젝트당 500만원~),
커스텀 바인딩 개발(건당 200~500만원), SLA 유지보수 계약(월 100~300만원).
진입 전략은 GitHub 기여로 이름을 알린 뒤 "honker 컨트리뷰터" 타이틀로 영업하는 것.

**9) 니치 바인딩 개발 + 스폰서십** — 비어 있는 언어: PHP/Laravel(기회도 최상),
Swift(iOS/macOS), Dart/Flutter, Zig, R. GitHub Sponsors / Open Collective / Polar.sh.

### 추천 3단 로드맵

```
1단계 (1개월)  Honker Studio 대시보드
  - React로 구현 가능
  - 오픈소스 공개로 인지도 확보
  - Pro 라이선스 $99로 첫 수익

2단계 (2개월)  PHP / Laravel 드라이버
  - 본체에 PR → 공식 컨트리뷰터
  - Laravel 커뮤니티 인지도
  - Nova 패널 유료화 + 컨설팅 유입

3단계 (3개월~) AI 에이전트 런타임
  - 1·2단계의 신뢰와 이해도를 활용
  - 가장 큰 시장, 좋은 타이밍
  - 클라우드 싱크 구독 + 엔터프라이즈 계약
```

### 리스크와 대응

| 리스크 | 대응 |
| --- | --- |
| honker가 Alpha 단계 | "Alpha 기반" 명시, 자체 테스트 강화, 버전 고정 |
| 원작자가 직접 유료 대시보드를 낼 수 있음 | 선점 또는 협업 제안(수익 배분) |
| 단일 머신 한계로 대형 고객 불가 | 처음부터 "소규모 팀 전용"으로 포지셔닝 |
| SQLite 큐 시장이 작을 수 있음 | 교육 콘텐츠로 시장 자체를 키우기 |
| 프로젝트 중단 가능성 | 포크 유지보수 능력 확보 |

---

## 13. 한 줄 요약

> 서버 한 대로 돌릴 서비스라면 Redis를 지우고 honker를 쓸 수 있다.
> 파일 하나로 큐·pub/sub·스트림·스케줄러가 끝나고,
> "DB는 저장됐는데 작업은 유실"이라는 버그가 구조적으로 불가능해진다.
> 단, Alpha 단계이고 단일 머신 전용이라는 두 가지 전제를 반드시 지켜야 한다.
