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

Текущая тестовая route:

- `POST /integration/api/v1/users`

Текущий тестовый request body:

```json
{
  "date_from": "2026-05-01",
  "date_to": "2026-05-29"
}
```

Текущее поведение:

1. запрос валидируется;
2. по телу строится `dedupe_key`;
3. выполняется поиск активной job;
4. если активной job нет:
   - создается `prepare` job со статусом `received`;
   - создается outbox record для topic `integration.prepare`;
   - возвращается `202 Accepted`;
5. если активная job уже есть:
   - возвращается существующий `job_id`;
   - выставляется `reused=true`;
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
9. создается `delivery` job;
10. создается outbox record для topic `integration.delivery`.

Текущая тестовая реализация сбора данных:

- вместо полноценной агрегации данных вызывается внешний product API;
- его ответ используется как итоговый большой JSON.

Текущая тестовая интеграция:

- `GET https://lk.sps38.pro/api/worksheets/export`
- query params:
  - `date_from`
  - `date_to`
- headers:
  - `Authorization: Bearer <token>`
  - `Accept: application/json`

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

- если уже существует активная job с тем же `dedupe_key`, новая цепочка не создается.

Поле ответа API:

- `reused=false` означает, что была создана новая heavy job;
- `reused=true` означает, что уже существовала активная job и была возвращена именно она.

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

- `job + outbox` пока не пишутся в одной DB transaction;
- `integration_inbox` пока нет;
- финального outbound HTTP sender пока нет;
- полноценной retry classification / DLQ behavior пока нет;
- naming route пока тестовый и должен быть переименован в предметный endpoint.
