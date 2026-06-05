# Integration Platform Architecture v3 (Final)

## Цель

Integration Platform — единая платформа для интеграции внутренних и внешних систем компании.

Платформа должна обеспечивать:

- обработку легких запросов;
- обработку тяжелых запросов;
- обмен данными между системами;
- надежную доставку сообщений;
- идемпотентность;
- безопасность;
- масштабируемость;
- аудит и восстановление после ошибок.

Платформа не привязана к конкретной системе.

Поддерживаемые сценарии:

- 1С
- Sigur
- СБИС
- Диадок
- Telegram
- Email
- внутренние сервисы компании

---

# Основные принципы

Платформа разделяет запросы на:

```text
Light Request
Heavy Request
```

---

# Light Request

Используется для быстрых синхронных операций.

Примеры:

- получение справочников;
- получение статусов;
- небольшие выборки данных;
- служебные запросы.

Схема:

```text
Client
  │
  ▼
HTTP Request
  │
  ▼
Handler
  │
  ▼
UseCase
  │
  ▼
Response
```

Особенности:

- выполняется синхронно;
- не использует RabbitMQ;
- результат возвращается сразу.

---

# Heavy Request

Используется для длительных операций.

Примеры:

- месячные отчеты;
- массовые выгрузки;
- генерация документов;
- импорт данных;
- сверка данных;
- синхронизация внешних систем.

Схема:

```text
Client
  │
  ▼
Gateway
  │
  ▼
Auth + Validate + Idempotency
  │
  ▼
Create Job
  │
  ▼
Outbox
  │
  ▼
RabbitMQ
  │
  ▼
Prepare Worker
  │
  ▼
Storage
  │
  ▼
Create Delivery Job
  │
  ▼
Outbox
  │
  ▼
RabbitMQ
  │
  ▼
Delivery Worker
  │
  ▼
External System
```

---

# Разделение ответственности

## transport/http

Отвечает только за:

- HTTP;
- DTO;
- валидацию формата;
- вызов Engine.

Не содержит бизнес-логики.

---

## Engine

Отвечает за orchestration.

Функции:

- создание Job;
- запуск Workflow;
- работа со статусами;
- работа с очередями;
- retry;
- dedupe;
- idempotency;
- replay.

Не содержит бизнес-логики.

---

## Workflow

Содержит бизнес-логику.

Примеры:

```text
monthly_report
employee_sync
ticket_export
sigur_sync
notification_send
```

Workflow знает:

- как собрать данные;
- как преобразовать данные;
- как сформировать результат.

---

# Inbound и Outbound

Каждая интеграция может работать в двух направлениях.

```text
Inbound
External System → Platform
```

```text
Outbound
Platform → External System
```

Поле:

```text
direction
```

Возможные значения:

```text
inbound
outbound
```

---

# Job Lifecycle

Полный жизненный цикл:

```text
received
  │
validated
  │
prepared
  │
delivering
  │
done
```

Ошибки:

```text
retrying
failed
dlq
```

---

# Job Store

Таблица:

```text
integration_jobs
```

Поля:

```text
id
correlation_id
parent_id

type
kind
direction

status

dedupe_key
idempotency_key

payload_json
result_path

attempts
last_error

created_at
started_at
finished_at
```

---

# Correlation ID

Используется для трассировки всей цепочки.

Пример:

```text
Request
 ↓
Prepare Job
 ↓
Delivery Job
```

Все используют один correlation_id.

---

# Kind

Тип задачи.

```text
prepare
delivery
```

Пример:

```text
kind=prepare
```

```text
kind=delivery
```

---

# Idempotency

Критически важный механизм.

Dedupe не является идемпотентностью.

Для каждого внешнего запроса хранится:

```text
idempotency_key
```

Таблица:

```text
integration_inbox
```

Поля:

```text
idempotency_key
source
response
created_at
```

Повторный запрос:

```text
не запускает обработку повторно
```

а возвращает уже сохраненный результат.

---

# Dedupe

Защищает от создания одинаковых активных задач.

Пример:

```text
monthly_report:2026-06-01:2026-06-30:all
```

Если существует:

```text
pending
processing
retrying
```

новая задача не создается.

---

# Ordering и Versioning

Для мастер-данных используется версия.

Поля:

```text
source_changed_at
version
```

Правило:

```text
старые события
не могут перезаписать новые
```

Это особенно важно для:

- сотрудников;
- Sigur;
- прав доступа;
- справочников.

---

# Outbox Pattern

Обязательная часть платформы.

Проблема:

```text
DB Commit OK
Rabbit Publish FAIL
```

Результат:

```text
потеря события
```

Решение:

```text
Transaction
 ├── Business Data
 └── Outbox Record
```

Отдельный воркер публикует сообщения из Outbox.

---

# RabbitMQ

Очереди:

```text
1c.prepare
1c.delivery

sigur.prepare
sigur.delivery

integration.retry
integration.dlq
```

Сообщение:

```json
{
  "job_id": "uuid"
}
```

Большие данные в RabbitMQ не передаются.

---

# Storage

Результаты тяжелых задач хранятся отдельно.

Пример:

```text
/storage/reports/{job_id}.json
```

В БД хранится только:

```text
result_path
```

---

# Delivery

После успешной подготовки данных создается Delivery Job.

Схема:

```text
Prepare Job
 ↓
Delivery Job
 ↓
External System
```

Delivery отвечает за:

- отправку данных;
- повторные попытки;
- контроль доставки.

---

# Retry

Используется только для идемпотентных операций.

Ошибки:

```text
500
502
503
504
timeout
connection refused
```

Стратегия:

```text
Exponential Backoff
+
Jitter
```

Пример:

```text
30 сек
2 мин
10 мин
30 мин
```

После превышения лимита:

```text
DLQ
```

---

# Replay

Любая задача из:

```text
failed
dlq
```

может быть повторно запущена вручную.

Это обязательная часть платформы.

---

# Scheduler

Поддерживает:

```text
cron jobs
polling integrations
scheduled sync
```

Особенно важен для Sigur и других pull-интеграций.

---

# Security

Обязательный раздел.

## Аутентификация

Только сервисная.

Поддержка:

```text
mTLS
Service Token
```

Пользовательские сессии запрещены.

---

## Подпись запросов

Используется:

```text
HMAC SHA256
```

Проверяется до обработки payload.

---

## Gateway Protection

Используется:

```text
IP Allow List
Rate Limit
Quota Per Connector
```

---

## Защита данных

Используется:

```text
Encryption At Rest
Retention Policy
```

ПДн не должны попадать в логи.

---

# Audit Log

Любое изменение должно быть зафиксировано.

Пример:

```text
кто
когда
что изменил
из какой системы
```

---

# Observability

Платформа должна предоставлять:

## Метрики

```text
queue size
processing time
retry count
dlq count
```

## Логи

```text
correlation_id
job_id
connector
```

## Трейсинг

```text
OpenTelemetry
```

---

# Будущее развитие

- Circuit Breaker
- Event Bus
- Webhook Engine
- Workflow Designer
- Distributed Cache
- Multi Region Processing

---

# Ключевые преимущества

Технические:

- единая архитектура интеграций;
- масштабируемость;
- отказоустойчивость;
- идемпотентность;
- аудит;
- replay;
- централизованная обработка ошибок;
- безопасная работа с данными.

Бизнес:

- быстрое подключение новых систем;
- снижение стоимости поддержки;
- единая точка мониторинга;
- снижение количества ошибок;
- независимость от конкретных интеграций.
