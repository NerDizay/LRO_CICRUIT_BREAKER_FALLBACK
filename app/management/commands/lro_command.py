import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

from django.core.management.base import BaseCommand

from app.models import Task
from app.repository import claim_pending_tasks, save_task_error, save_task_result
from app.services import CircuitBreaker, CircuitBreakerOpen, LROClient


TASK_TTL_SECONDS = 30
EXPECTED_PEAK_RPS = 2
PROCESSING_TIME = 10
CONCURRENCY = EXPECTED_PEAK_RPS * PROCESSING_TIME * 2

class LROWorker:
    def __init__(
        self,
        concurrency: int,
        executor_workers: int = 4,
        cb_failure_threshold: int = 5,
        cb_open_timeout: int = 30,
    ) -> None:
        cb = CircuitBreaker(failure_threshold=cb_failure_threshold, open_timeout=cb_open_timeout)
        self.client = LROClient(circuit_breaker=cb)
        self.executor = ThreadPoolExecutor(max_workers=executor_workers)
        self.queue: asyncio.Queue[Task] = asyncio.Queue()
        self.concurrency = concurrency
        self._shutdown = False
        self._stdout_log = False

    def _log(self, msg: str) -> None:
        if self._stdout_log:
            sys.stdout.write(msg + '\n')
            sys.stdout.flush()

    def _claim_pending_tasks(self, limit: int) -> list[Task]:
        return claim_pending_tasks(limit, TASK_TTL_SECONDS)

    async def _process_task(self, task: Task) -> None:
        self._log(f'Processing task {task.task_id}, queue: {self.queue.qsize()}')
        payload: dict = {'text': task.text}

        try:
            resp1 = await self.client.method_one_async(payload)
        except CircuitBreakerOpen:
            resp2 = await self.client.method_fallback_async(payload)
            await self._save_result(task, None, resp2, Task.Status.DONE_FALLBACK)
            return
        except Exception as e:
            await self._save_error(task, payload, 'method_one_async', type(e).__name__)
            resp2 = await self.client.method_fallback_async(payload)
            await self._save_result(task, None, resp2, Task.Status.DONE_FALLBACK)
            return

        try:
            payload2 = {'previous': resp1}
            resp2 = await self.client.method_two_async(payload2)
        except CircuitBreakerOpen:
            resp2 = await self.client.method_fallback_async(payload)
            await self._save_result(task, resp1, resp2, Task.Status.DONE_FALLBACK)
            return
        except Exception as e:
            await self._save_error(task, {'previous': resp1}, 'method_two_async', type(e).__name__)
            resp2 = await self.client.method_fallback_async(payload)
            await self._save_result(task, resp1, resp2, Task.Status.DONE_FALLBACK)
            return

        await self._save_result(task, resp1, resp2)

    async def _save_result(self, task: Task, resp1: dict | None, resp2: dict, status: Task.Status = Task.Status.DONE_USUAL) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: save_task_result(task, resp1, resp2, status),
        )
        self._log(f'Saved result for task {task.task_id} (status={status.name})')

    async def _save_error(self, task: Task, payload: dict, err_method: str, err_msg: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: save_task_error(task, payload, err_method, err_msg),
        )
        self._log(f'Saved error for task {task.task_id} ({err_method}: {err_msg})')

    async def worker(self, worker_id: int) -> None:
        self._log(f'Worker {worker_id} started')
        try:
            while not self._shutdown:
                try:
                    task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                await self._process_task(task)
                self.queue.task_done()
        except asyncio.CancelledError:
            pass

    async def enqueue_tasks(self) -> int:
        loop = asyncio.get_running_loop()
        tasks = await loop.run_in_executor(self.executor, self._claim_pending_tasks, 50)
        for task in tasks:
            await self.queue.put(task)
        self._log(f'Enqueued {len(tasks)} tasks, queue: {self.queue.qsize()}')

    async def run(self) -> None:
        workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.concurrency)
        ]

        while not self._shutdown:
            await self.enqueue_tasks()
            await asyncio.sleep(1)

        # Ждём завершения текущих задач — но с таймаутом
        drain_task = asyncio.create_task(self.queue.join())
        try:
            await asyncio.wait_for(drain_task, timeout=30)
        except asyncio.TimeoutError:
            for w in workers:
                w.cancel()

        # Ждём пока воркеры завершатся
        await asyncio.gather(*workers, return_exceptions=True)

    def shutdown(self) -> None:
        self._shutdown = True


class Command(BaseCommand):
    help = 'LRO background worker'

    def handle(self, *args, **options):
        import sys
        loop = asyncio.new_event_loop()
        worker = LROWorker(
            concurrency=CONCURRENCY,
            cb_failure_threshold=5,
            cb_open_timeout=30,
        )

        def signal_handler():
            sys.stdout.write('\nGraceful shutdown requested...\n')
            sys.stdout.write('Press Ctrl+C again for force exit\n')
            worker.shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        self.stdout.write('Starting LRO worker...')
        try:
            loop.run_until_complete(worker.run())
        except KeyboardInterrupt:
            sys.stdout.write('\n⚡ Force exit!\n')
            for task in asyncio.all_tasks(loop):
                task.cancel()
            loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
        finally:
            loop.run_until_complete(worker.client.close())
            worker.executor.shutdown(wait=False)
            loop.close()
            self.stdout.write('Worker stopped')
