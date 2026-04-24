# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Project-Specific Guidelines

### Tech Stack
- **Python 3.12 + FastAPI + Pydantic v2 + pydantic-settings**
- 运行方式：uvicorn，Docker 多阶段构建（builder → slim），部署于 K8s
- 异步优先：路由函数用 `async def`，阻塞 IO 用 `asyncio.to_thread` 包装

### 项目结构

```
app/
  main.py              # 应用工厂 create_app()，lifespan 管理生命周期
  conf/config.py       # pydantic-settings Settings，环境变量 + .env
  conf/olap_config.json# 热更新配置（无需重启）
  core/
    api_schema.py      # StandardResponse / ErrorResponse / BaseRequest
    exceptions.py      # BizException + 全局异常处理器
    middleware.py       # request_id_middleware, auth_middleware
    request_context.py  # ContextVar (trace_id, username)
  api/
    router.py           # 中央路由注册，按领域 include_router
    v1/                  # 各领域路由模块（olap, datasets, mtp_eval 等）
context/                 # 外部连接器（auth_client, doris_connector）
src/domains/             # 领域业务逻辑（kv, datasets, mtp_eval, process_scheduler）
scripts/                 # 离线脚本 / Pipeline（run_pipeline, daily_report 等）
```

### 关键约定

1. **统一响应格式**：所有 API 返回 `StandardResponse(code=0, message=..., data=..., trace_id=...)` 或 `ErrorResponse(code=非0, message=..., detail=..., trace_id=...)`
2. **异常处理**：业务异常抛 `BizException(status_code, code, message, detail)`，由全局 handler 统一兜底，勿在路由中手动构造错误 JSON
3. **路由注册**：在 `app/api/v1/` 下新建模块，在 `app/api/router.py` 中 `include_router(prefix="/xxx", tags=["xxx"])`
4. **鉴权**：Bearer Token → `auth_middleware` 自动注入 `request.state.user/token/groups`；公开路径加到 `PUBLIC_PATHS` 或 `PUBLIC_PATH_PREFIXES`
5. **链路追踪**：`request_id_middleware` 自动注入 `request.state.trace_id`，日志通过 `ContextVar` 携带
6. **配置**：环境变量 / `.env` → `settings` 单例；运行时热更新参数放 `olap_config.json`
7. **日志**：`logging.getLogger(__name__)`，勿用 print；日志中务必携带 `trace_id`

### 开发注意事项
- 新增路由模块后需同步注册到 `app/api/router.py`
- `context/` 放外部系统连接器，`src/domains/` 放业务逻辑，勿混放在路由文件中
- Dockerfile 采用多阶段构建，新增系统依赖需分别加到 builder 和 runtime 阶段
- C++ 编译产物（如 `cache_hit_rate`）在 builder 阶段编译，COPY 到 runtime 阶段
