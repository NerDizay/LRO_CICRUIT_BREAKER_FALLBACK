from django.core.files.uploadedfile import UploadedFile
from django.db import transaction

from .models import Task, TaskLog


def create_task(owner_id: str, text: str, audio: UploadedFile | None = None) -> Task:
    return Task.objects.create(owner_id=owner_id, text=text, audio=audio)


def claim_pending_tasks(limit: int, task_ttl_seconds: int) -> list[Task]:
    """Атомарно забираем PENDING задачи за последние N секунд и переводим в IN_PROGRESS."""
    from django.utils import timezone

    with transaction.atomic():
        cutoff = timezone.now() - timezone.timedelta(seconds=task_ttl_seconds)
        task_ids = list(
            Task.objects.select_for_update(skip_locked=True)
            .filter(status=Task.Status.PENDING, created_at__gte=cutoff)
            .order_by('-created_at')
            .values_list('task_id', flat=True)[:limit]
        )
        if not task_ids:
            return []
        Task.objects.filter(task_id__in=task_ids).update(
            status=Task.Status.IN_PROGRESS
        )
        return list(Task.objects.filter(task_id__in=task_ids))


def save_task_result(task: Task, resp1: dict | None, resp2: dict, status: Task.Status = Task.Status.DONE_USUAL) -> None:
    """Сохраняет логи успешного вызова и результат."""
    with transaction.atomic():
        logs = [
            TaskLog(task=task, method_name='method_two_async', payload={}, response=resp2),
        ]
        if resp1 is not None:
            logs.insert(0, TaskLog(
                task=task, method_name='method_one_async',
                payload={'text': task.text}, response=resp1,
            ))
        TaskLog.objects.bulk_create(logs)
        Task.objects.filter(task_id=task.task_id).update(
            result=resp2, status=status
        )


def save_task_error(task: Task, payload: dict, err_method: str, err_msg: str) -> None:
    """Сохраняет лог ошибки."""
    TaskLog.objects.create(
        task=task,
        method_name=err_method,
        payload=payload,
        response={'error': err_msg},
    )
