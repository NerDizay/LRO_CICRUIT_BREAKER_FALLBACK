from .models import Task


def create_task(owner_id: str, text: str, audio=None) -> Task:
    return Task.objects.create(owner_id=owner_id, text=text, audio=audio)
