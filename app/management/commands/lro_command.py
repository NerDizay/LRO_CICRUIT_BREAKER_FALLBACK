import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor

from django.core.management.base import BaseCommand
from django.db import transaction

from app.models import Task, TaskLog
from app.services import LROClient


class LROWorker:
    def __init__(self, concurrency: int, executor_workers: int = 4):
        self.client = LROClient()
        self.executor = ThreadPoolExecutor(max_workers=executor_workers)
        self.queue = asyncio.Queue()
        self.concurrency = concurrency
        self._shutdown = False

    def _log(self, msg: str):
        import sys
        sys.stdout.write(msg + '\n')
        sys.stdout.flush()

    def _claim_pending_tasks(self, limit=50):
        """Атомарно забираем PENDING задачи и переводим в IN_PROGRESS."""
        with transaction.atomic():
            task_ids = list(
                Task.objects.select_for_update(skip_locked=True)
                .filter(status=Task.Status.PENDING)
                .order_by('-created_at')
                .values_list('task_id', flat=True)[:limit]
            )
            if not task_ids:
                return []
            Task.objects.filter(task_id__in=task_ids).update(
                status=Task.Status.IN_PROGRESS
            )
            tasks = list(Task.objects.filter(task_id__in=task_ids))

        return tasks

    def _update_task_error(self, task_id, error):
        Task.objects.filter(task_id=task_id).update(
            status=Task.Status.ERROR,
            result={'error': str(error)},
        )

    async def _process_task(self, task):
        payload = {'text': task.text}

        resp1 = None
        resp2 = None
        err_method = None
        err_msg = None

        # 1-й асинхронный запрос
        try:
            resp1 = await self.client.method_one_async(payload)
        except Exception as e:
            err_method, err_msg = 'method_one_async', type(e).__name__

        # 2-ой асинхронный запрос (только если первый успешен)
        if resp1 is not None:
            try:
                payload2 = {'previous': resp1}
                resp2 = await self.client.method_two_async(payload2)
            except Exception as e:
                err_method, err_msg = 'method_two_async', type(e).__name__

        # Один раз сохраняем всё
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: self._save(task, payload, resp1, resp2, err_method, err_msg),
        )

        if err_method:
            self._log(f'Task {task.task_id} failed on {err_method}: {err_msg}')
        else:
            self._log(f'Task {task.task_id} processed successfully')

    def _save(self, task, payload, resp1, resp2, err_method, err_msg):
        """Сохраняет логи и результат одним батчем."""
        logs = []
        if resp1 is not None:
            logs.append(TaskLog(
                task=task, method_name='method_one_async',
                payload={'text': task.text}, response=resp1,
            ))
        if err_method == 'method_one_async':
            logs.append(TaskLog(
                task=task, method_name='method_one_async',
                payload={'text': task.text}, response={'error': err_msg},
            ))

        if resp2 is not None:
            logs.append(TaskLog(
                task=task, method_name='method_two_async',
                payload={'previous': resp1}, response=resp2,
            ))
        if err_method == 'method_two_async':
            logs.append(TaskLog(
                task=task, method_name='method_two_async',
                payload={'previous': resp1}, response={'error': err_msg},
            ))

        with transaction.atomic():
            if logs:
                TaskLog.objects.bulk_create(logs)

            if resp2 is not None:
                Task.objects.filter(task_id=task.task_id).update(
                    result=resp2, status=Task.Status.DONE_USUAL
                )
            else:
                Task.objects.filter(task_id=task.task_id).update(
                    status=Task.Status.ERROR,
                    result={'error': err_msg} if err_msg else {},
                )

    async def worker(self, worker_id: int):
        self._log(f'Worker {worker_id} started')
        loop = asyncio.get_running_loop()
        try:
            while not self._shutdown:
                try:
                    task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    await self._process_task(task)
                except Exception as e:
                    self._log(f'Error processing task {task.task_id}: {e}')
                    await loop.run_in_executor(
                        self.executor, self._update_task_error, task.task_id, e
                    )
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            pass
        self._log(f'Worker {worker_id} stopped')

    async def enqueue_tasks(self):
        loop = asyncio.get_running_loop()
        tasks = await loop.run_in_executor(self.executor, self._claim_pending_tasks, 50)
        for task in tasks:
            await self.queue.put(task)
        return len(tasks)

    async def run(self):
        # Запускаем воркеров
        workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.concurrency)
        ]

        # Цикл наполнения очереди
        while not self._shutdown:
            count = await self.enqueue_tasks()
            if count > 0:
                self._log(f'Enqueued {count} tasks, queue size: {self.queue.qsize()}')
            await asyncio.sleep(1)

        self._log('🛑 Drain queue, do not enqueue new tasks...')

        # Ждём завершения текущих задач — но с таймаутом
        drain_task = asyncio.create_task(self.queue.join())
        try:
            await asyncio.wait_for(drain_task, timeout=30)
        except asyncio.TimeoutError:
            self._log('⚠️  Drain timeout, forcing shutdown...')
            for w in workers:
                w.cancel()

        # Ждём пока воркеры завершатся
        await asyncio.gather(*workers, return_exceptions=True)
        self._log('All workers stopped.')

    def shutdown(self):
        self._shutdown = True


class Command(BaseCommand):
    help = 'LRO background worker'

    def handle(self, *args, **options):
        import sys
        loop = asyncio.new_event_loop()
        worker = LROWorker(concurrency=50)

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
