from rest_framework import serializers


class CreateTaskSerializer(serializers.Serializer):
    text = serializers.CharField(required=False, allow_blank=True)
    audio = serializers.FileField(required=False)

    def validate(self, attrs):
        text = attrs.get('text')
        audio = attrs.get('audio')

        if not text and not audio:
            raise serializers.ValidationError('text или audio должны быть переданы')

        return attrs


class GetTaskSerializer(serializers.Serializer):
    owner_id = serializers.UUIDField()
    task_id = serializers.UUIDField()


class CreateTaskResponseSerializer(serializers.Serializer):
    task_id = serializers.UUIDField()


class GetTaskResponseSerializer(serializers.Serializer):
    status = serializers.IntegerField()
    result = serializers.JSONField()
