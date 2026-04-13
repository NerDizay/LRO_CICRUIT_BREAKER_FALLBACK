from functools import wraps
from uuid import uuid7

from rest_framework.views import APIView
from rest_framework.parsers import FormParser, MultiPartParser, JSONParser
from rest_framework.response import Response
from rest_framework import status
from .serializers import (
    CreateTaskSerializer, GetTaskSerializer,
    CreateTaskResponseSerializer, GetTaskResponseSerializer,
)
from .models import Task
from .repository import create_task


def handle_anonymous_id(view_method):
    from uuid import UUID

    @wraps(view_method)
    def wrapper(self, request, *args, **kwargs):
        anon_id = request.headers.get('Anonymous-Id')
        if not anon_id:
            anon_id = str(uuid7())
        else:
            try:
                UUID(anon_id)
            except ValueError:
                anon_id = str(uuid7())
        request.anonymous_id = anon_id
        res = view_method(self, request, *args, **kwargs)
        res['Anonymous-Id'] = anon_id
        return res
    return wrapper


class CreateTaskView(APIView):
    parser_classes = (FormParser, MultiPartParser, JSONParser)

    @handle_anonymous_id
    def post(self, request):
        serializer = CreateTaskSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        task = create_task(
            owner_id=request.anonymous_id,
            text=serializer.validated_data.get('text', ''),
            audio=serializer.validated_data.get('audio'),
        )

        response_data = CreateTaskResponseSerializer({'task_id': task.task_id}).data
        return Response(response_data, status=status.HTTP_202_ACCEPTED)


class GetTaskView(APIView):
    def post(self, request):
        owner_id = request.headers.get('Anonymous-Id')
        if not owner_id:
            return Response({}, status=status.HTTP_404_NOT_FOUND)

        serializer = GetTaskSerializer(data={**request.data, 'owner_id': owner_id})
        serializer.is_valid(raise_exception=True)

        owner_id = serializer.validated_data['owner_id']
        task_id = serializer.validated_data['task_id']

        try:
            task = Task.objects.get(owner_id=owner_id, task_id=task_id)
        except Task.DoesNotExist:
            return Response({}, status=status.HTTP_404_NOT_FOUND)

        response_data = GetTaskResponseSerializer({
            'status': task.status,
            'result': task.result,
        }).data
        return Response(response_data)
