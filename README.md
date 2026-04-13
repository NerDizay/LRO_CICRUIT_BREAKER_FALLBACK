Репа для демонстрации некоторых архитектурных паттернов:
1. Long-Running Operation (LRO) Pattern
Классический паттерн асинхронных длительных операций: клиент отправляет запрос (POST /lro-create), сервер возвращает 202 Accepted с идентификатором задачи, а клиент периодически опрашивает статус (POST /lro-get). Это отделяет приём запроса от его выполнения.

2. Circuit Breaker
Классическая реализация паттерна Мартина Фаулера. Три состояния (фактически два — CLOSED/OPEN с автоматическим переходом):
Считает ошибки (_failures)
При превышении порога (failure_threshold=5) переходит в OPEN
После таймаута (open_timeout=30) автоматически возвращается в CLOSED
Выбрасывает CircuitBreakerOpen при открытом состоянии

3. Fallback Pattern
При отказе основного метода (method_one_async, method_two_async) вызывается альтернативная стратегия — method_fallback_async, возвращающая заглушку {'fallback': 'fallback'}.

4. TTL (Time-To-Live) Pattern
Задачи с created_at старше TASK_TTL_SECONDS = 30 не забираются в обработку — защита от зависших задач.

5. Graceful Shutdown Pattern
Обработка SIGINT/SIGTERM:
Установка флага _shutdown = True
queue.join() с таймаутом 30 секунд
Cancel воркеров при превышении таймаута
asyncio.gather(*workers, return_exceptions=True)
Закрытие HTTP-клиента и executor

6. Pessimistic Concurrency Control (via Row-Level Locking)
Task.objects.select_for_update(skip_locked=True)
SELECT ... FOR UPDATE SKIP LOCKED — пессимистическая блокировка строк с пропуском уже занятых. Несколько воркеров могут параллельно забирать задачи без конфликтов и ожидания.

7. UUID7 as Primary Key Pattern
Использование UUID7 (временно-сортируемый UUID) вместо автоинкремента — предотвращает hot-spot на PRIMARY KEY индексе в MySQL, обеспечивает глобальную уникальность.

и другие.

Ключевые файлы:
1. [Основная команда с asyncio](app/management/commands/lro_command.py)
2. [Репозиторий](app/repository.py)
3. [Circuit breaker + Client with LRO](app/services.py)

