# Issue Resolution: asyncio: Task Spawned into Already-Cancelled TaskGroup Hangs Indefinitely

## 1. Upstream Metadata
- **Repository:** `agronholm/anyio`
- **Issue Reference:** [#1486](https://github.com/agronholm/anyio/issues/1486)
- **Title:** `asyncio: task spawned into an already-cancelled task group is never cancelled, which can hang forever`
- **Component:** `src/anyio/_backends/_asyncio.py` (`CancelScope`, `TaskGroup._spawn`)

---

## 2. Problem Description & Root Cause
On the `asyncio` backend, when a task was spawned (`tg.start_soon` or `tg.start`) into a task group whose `cancel_scope` (or an unshielded enclosing cancel scope) was already cancelled, the newly spawned task was never cancelled if, at that moment, nothing else in the scope was cancellable.

### Root Cause:
1. `CancelScope._deliver_cancellation()` schedules subsequent cancellation passes via `loop.call_soon` only as long as `should_retry` is `True`. Once all contained tasks under the scope were either finished, blocked in shielded scopes, or completed, `_deliver_cancellation` sets `self._cancel_handle = None` and terminates the scheduled loop.
2. In `TaskGroup._spawn()`, a new child task was appended to `initial_scope._tasks` and `self._tasks`.
3. However, `_spawn()` did not notify or restart cancellation delivery on `initial_scope` or its cancelled ancestors.
4. Consequently, the newly spawned task ran uncancelled indefinitely. When other tasks (such as a worker executing shielded cleanup) waited for the child task, they deadlocked / hung forever. In contrast, on the `trio` backend, child tasks spawned into cancelled scopes are properly cancelled at their first checkpoint.

---

## 3. Reproduction Test
A reproduction test `test_task_spawned_into_already_cancelled_taskgroup` was added to `tests/test_taskgroups.py`:
```python
async def test_task_spawned_into_already_cancelled_taskgroup() -> None:
    helper_done = Event()

    async def helper() -> None:
        try:
            await sleep_forever()
        finally:
            helper_done.set()

    async def worker() -> None:
        try:
            await sleep_forever()
        finally:
            with move_on_after(5, shield=True) as cleanup:
                for _ in range(10):
                    await checkpoint()
                tg.start_soon(helper)
                await helper_done.wait()

            assert not cleanup.cancelled_caught

    async with create_task_group() as tg:
        tg.start_soon(worker)
        await wait_all_tasks_blocked()
        tg.cancel_scope.cancel()
```
Prior to the fix:
- On `trio`: passed (`ok`).
- On `asyncio`: timed out / hung (`asyncio: hung`), failing the test.

---

## 4. Code Fix
In `src/anyio/_backends/_asyncio.py`:
1. Introduced `CancelScope._restart_cancellation()` to traverse from the current scope upward through unshielded parent scopes, finding any cancelled scope whose `_cancel_handle is None` and restarting `_deliver_cancellation(scope)`.
2. Updated `CancelScope._restart_cancellation_in_parent()` to delegate to `self._parent_scope._restart_cancellation()`.
3. In `TaskGroup._spawn()`, immediately after adding the newly created task to `initial_scope._tasks`, called `initial_scope._restart_cancellation()`.

---

## 5. Verification Results
- **Reproduction Test:** `3 passed, 424 deselected in 0.44s` (`tests/test_taskgroups.py -k test_task_spawned_into_already_cancelled_taskgroup`).
- **Task Groups Test Suite:** `418 passed, 6 skipped, 3 xfailed in 17.38s` (`tests/test_taskgroups.py`).
- **Linter Check:** `ruff check` passed with zero errors.
- **Repository Commit:** Pushed cleanly to `rajanjava/anyio-private` on branch `main`.
