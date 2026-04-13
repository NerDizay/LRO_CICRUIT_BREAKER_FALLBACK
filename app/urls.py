from django.urls import path
from .views import CreateTaskView, GetTaskView

urlpatterns = [
    path('lro-create', CreateTaskView.as_view()),
    path('lro-get', GetTaskView.as_view()),
]
