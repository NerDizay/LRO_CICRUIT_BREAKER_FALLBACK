from django.db import models
from uuid import uuid7


class Task(models.Model):
    class Status(models.IntegerChoices):
        PENDING = 0, 'Pending'
        IN_PROGRESS = 4, 'In Progress'
        DONE_USUAL = 1, 'Done Usual'
        DONE_FALLBACK = 2, 'Done Fallback'
        ERROR = 3, 'Error'

    task_id = models.UUIDField(default=uuid7, editable=False, primary_key=True)
    owner_id = models.UUIDField(default=uuid7, editable=False)
    text = models.TextField(blank=True, default='')
    audio = models.FileField(upload_to='audio/', blank=True, null=True)
    status = models.IntegerField(choices=Status.choices, default=Status.PENDING)
    result = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'Task {self.task_id} - {self.get_status_display()}'

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status'], name='idx_task_status'),
            models.Index(fields=['owner_id', 'task_id'], name='idx_task_owner_task'),
        ]


class TaskLog(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE, related_name='logs')
    method_name = models.CharField(max_length=255)
    payload = models.JSONField()
    response = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'Log for Task {self.task.task_id} - {self.method_name}'
