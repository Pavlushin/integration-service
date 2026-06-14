# Текущий Runtime Интеграции

## Назначение

`onec-integration` — это Go-сервис для обработки долгих интеграционных запросов.

Текущий фокус MVP:

- принять тяжелый HTTP-запрос;
- превратить его в job;
- провести выполнение через RabbitMQ workers;
- на этапе `prepare` собрать данные и сформировать большой JSON;
- сохранить этот JSON в локальный файл;
- на этапе `delivery` открыть готовый файл и доставить его содержимое;
- завершить цепочку выполнения.

Это не CRUD-сервис. Центр архитектуры:

- `job`
- `outbox`
- `queue`
- `worker`
- `storage`

## Текущие компоненты

### API

Точка входа:

- [`cmd/api/main.go`](/Users/dimshewik/GolandProjects/onec-integration/cmd/api/main.go)

Ответственность:

- принимать HTTP-запросы;
- назначать `request_id` и `correlation_id`;
- валидировать payload;
- создавать `prepare` job;
- создавать outbox record;
- возвращать `202 Accepted`.

### Worker

Точка входа:

- [`cmd/worker/main.go`](/Users/dimshewik/GolandProjects/onec-integration/cmd/worker/main.go)

Ответственность:

- отправлять pending outbox records в RabbitMQ;
- читать `integration.prepare`;
- запускать prepare processor;
- читать `integration.delivery`;
- запускать delivery processor.

### Postgres

Таблицы:

- `integration_jobs`
- `integration_outbox`

Миграции:

- [`migrations/000001_create_integration_jobs.up.sql`](/Users/dimshewik/GolandProjects/onec-integration/migrations/000001_create_integration_jobs.up.sql)
- [`migrations/000002_create_integration_outbox.up.sql`](/Users/dimshewik/GolandProjects/onec-integration/migrations/000002_create_integration_outbox.up.sql)

### RabbitMQ

Текущие очереди:

- `integration.prepare`
- `integration.delivery`

Контракт сообщения:

```json
{
  "job_id": "uuid"
}
```

Большие payload в RabbitMQ не отправляются.

### Файловое хранилище

Текущая реализация:

- [`internal/storage/filesystem.go`](/Users/dimshewik/GolandProjects/onec-integration/internal/storage/filesystem.go)

Текущий путь хранения:

- `out/json`

Поведение:

- на этапе `prepare` большой JSON сохраняется во временный файл `out/json/tmp/<prepare_job_id>.json`
- на этапе `delivery` worker открывает этот файл и работает уже с его содержимым
- в текущей тестовой реализации итоговый результат сохраняется в `out/json/<delivery_job_id>.json`
- временный файл удаляется после успешного завершения `delivery`

## Текущий Heavy Flow

### Этап запроса

Назначение:

- принять команду на запуск тяжелой обработки;
- создать `prepare` job;
- поставить ее в очередь;
- сразу вернуть `job_id` и `correlation_id`, не дожидаясь результата.

Текущая route для первого предметного export:

- `POST /integration/api/v1/worksheets/users-reports/export`

Для обратной совместимости пока также оставлен legacy alias:

- `POST /integration/api/v1/users`

Текущий request body:

```json
{
  "date_from": "2026-05-01",
  "date_to": "2026-05-29",
  "location_code_1c": "СПЗК-0047",
  "employee_code_1c": "EMP001"
}
```

Текущее поведение:

1. запрос валидируется;
2. по телу строится `dedupe_key`;
3. если передан `X-Idempotency-Key`, выполняется поиск сохраненного response в `integration_inbox`;
4. если response найден:
   - возвращается сохраненный `job_id` / `correlation_id`;
   - выставляется `reused=true`;
   - новая job и новое сообщение в очередь не создаются;
5. если response не найден, выполняется поиск последней job с тем же `dedupe_key`;
6. если reusable job нет:
   - `prepare` job со статусом `received`, outbox record для topic `integration.prepare` и inbox response создаются в одной DB transaction;
   - возвращается `202 Accepted`;
7. если reusable job уже есть:
   - возвращается существующий `job_id`;
   - выставляется `reused=true`;
   - новый `X-Idempotency-Key`, если он был передан, сохраняется в `integration_inbox`;
   - новое сообщение в очередь не создается.

### Этап Prepare

Processor:

- [`internal/engine/prepare_processor.go`](/Users/dimshewik/GolandProjects/onec-integration/internal/engine/prepare_processor.go)

Целевая роль этапа:

- собрать данные из внутренних или внешних источников;
- сформировать большой JSON;
- сохранить JSON в файл;
- подготовить `delivery` job для последующей отправки результата.

Текущее поведение:

1. читается сообщение из `integration.prepare`;
2. из Postgres загружается `prepare` job;
3. job переводится в `validated`;
4. выполняется сбор данных;
5. формируется большой JSON;
6. JSON сохраняется во временный файл;
7. обновляется `prepare.result_path`;
8. `prepare` job переводится в `prepared`;
9. `delivery` job и outbox record для topic `integration.delivery` создаются в одной DB transaction.

Текущая реализация сбора данных для `worksheets/users-reports`:

- при `WORKSHEETS_EXPORT_SOURCE=lk_mariadb` worker подключается к LK MariaDB и собирает JSON из таблиц worksheets;
- при `WORKSHEETS_EXPORT_SOURCE=disabled` worker стартует без LK MariaDB, но worksheets prepare job завершается явной non-retryable configuration error со статусом `failed`;
- режим `disabled` предназначен для локального/E2E запуска инфраструктуры без доступа к LK и не подменяет реальные данные.

Обязательные настройки для LK-backed режима:

- `WORKSHEETS_EXPORT_SOURCE=lk_mariadb`
- `LK_MARIADB_HOST`
- `LK_MARIADB_DATABASE`
- `LK_MARIADB_USER`
- `LK_MARIADB_PASSWORD`

В терминах архитектуры это не меняет роль этапа:

- `prepare` отвечает именно за получение данных и сборку большого JSON-файла.

### Этап Delivery

Processor:

- [`internal/engine/delivery_processor.go`](/Users/dimshewik/GolandProjects/onec-integration/internal/engine/delivery_processor.go)

Целевая роль этапа:

- открыть ранее собранный JSON-файл;
- доставить его содержимое во внешнюю систему;
- завершить цепочку выполнения.

Текущее поведение:

1. читается сообщение из `integration.delivery`;
2. из Postgres загружается `delivery` job;
3. job переводится в `delivering`;
4. открывается временный JSON-файл из `prepare.result_path`;
5. worker работает уже с содержимым файла, а не с данными из RabbitMQ;
6. в текущей тестовой реализации содержимое сохраняется в итоговый локальный файл;
7. обновляется `delivery.result_path`;
8. удаляется временный файл;
9. `delivery` job переводится в `done`;
10. родительская `prepare` job тоже переводится в `done`.

Текущая тестовая реализация `delivery`:

- вместо отправки во внешнюю систему JSON пока сохраняется в `out/json/<delivery_job_id>.json`.

Целевая боевая реализация `delivery`:

- открыть файл;
- прочитать JSON;
- отправить JSON во внешний endpoint;
- при успехе завершить chain;
- при ошибке перейти в retry / failed / dlq по policy.

## Текущая модель статусов

Используемые статусы:

- `received`
- `validated`
- `prepared`
- `delivering`
- `done`
- `retrying`
- `failed`
- `dlq`

Текущий happy path:

```text
prepare: received -> validated -> prepared -> done
delivery: prepared -> delivering -> done
```

## Семантика Dedupe

Текущее правило:

- если уже существует активная job с тем же `dedupe_key`, новая цепочка не создается;
- если последняя job с тем же `dedupe_key` успешно завершена (`done`), новая цепочка также не создается;
- если последняя job завершилась неуспешно (`dlq`/`failed`), повторный запрос может создать новую цепочку.

Поле ответа API:

- `reused=false` означает, что была создана новая heavy job;
- `reused=true` означает, что уже существовала reusable job и была возвращена именно она.

## Логирование и трассировка

Логи пишутся в:

- stdout
- `out/logs/*.log`

Каждую цепочку запроса можно отслеживать по:

- `correlation_id`
- `job_id`

Текущие полезные команды:

```bash
make log-tail
make logs-corr CORR=<correlation_id>
make logs-job JOB=<job_id>
make trace CORR=<correlation_id>
make db-jobs-full
make db-outbox
```

`make trace` выводит:

- связанные jobs из Postgres;
- связанные outbox records из Postgres;
- связанные логи из `out/logs`.

## Контракт для внешних сервисов, использующих Heavy Requests

### Формат запроса

Внешний сервис отправляет:

```http
POST /integration/api/v1/users
Content-Type: application/json
X-Correlation-ID: optional
X-Idempotency-Key: optional
```

Тело:

```json
{
  "date_from": "2026-05-01",
  "date_to": "2026-05-29"
}
```

### Немедленный ответ

Сервис отвечает сразу с `202 Accepted`.

Тело ответа:

```json
{
  "job_id": "uuid",
  "correlation_id": "uuid",
  "reused": false
}
```

Значение полей:

- `job_id`: идентификатор текущей heavy job chain;
- `correlation_id`: сквозной trace identifier всей цепочки;
- `reused`: была ли запущена новая цепочка или переиспользована уже существующая активная.

### Как внешний сервис должен интерпретировать ответ

Если `reused=false`:

- была запущена новая heavy-обработка.

Если `reused=true`:

- такой же запрос уже находится в обработке;
- вызывающая сторона должна использовать возвращенный `job_id`, а не ожидать новую отдельную обработку.

### Как внешний сервис должен получать результат heavy-обработки

В целевой схеме внешний сервис не должен ждать большой JSON в ответе на исходный POST-запрос.

Правильная модель такая:

1. внешний сервис вызывает heavy endpoint;
2. сразу получает `202 Accepted` и `job_id`;
3. дальше результат приходит отдельным delivery-каналом.

Возможные delivery-варианты:

- HTTP callback;
- отдельный outbound endpoint;
- запись в файловое хранилище;
- публикация в другую очередь;
- иная интеграционная доставка.

Текущая тестовая реализация результата:

- delivery worker читает файл из `prepare`;
- сохраняет итоговый JSON локально;
- тем самым подтверждает, что heavy chain полностью завершилась.

### Текущее ограничение

Публичного endpoint для проверки статуса job пока нет.

На текущем этапе статус можно наблюдать внутренне через:

- логи;
- `integration_jobs`;
- `integration_outbox`.

## Текущие технические ограничения

Пока еще не закрыты следующие пункты:

- финального outbound HTTP sender пока нет;
- retry classification пока базовая;
- naming route пока тестовый и должен быть переименован в предметный endpoint.
