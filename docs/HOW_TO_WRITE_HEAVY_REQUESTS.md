# Как Писать Heavy Requests

## Назначение

Этот документ описывает, как в текущем проекте добавлять новые heavy-запросы.

Под heavy-запросом в этом сервисе понимается не просто “долгий endpoint”, а полный асинхронный pipeline:

```text
HTTP request
-> create prepare job
-> outbox
-> RabbitMQ
-> prepare worker
-> JSON file
-> delivery worker
-> отправка содержимого файла во внешний сервис
```

Ключевой принцип:

- `prepare` отвечает за сбор данных и формирование большого JSON-файла
- `delivery` отвечает за чтение файла и доставку его содержимого во внешний сервис

Ни `prepare`, ни `delivery` не должны гонять большой JSON через RabbitMQ.

---

## Общая модель heavy-запроса

### 1. HTTP слой

HTTP endpoint:

- принимает входной запрос;
- валидирует payload;
- строит `dedupe_key`;
- вызывает `engine.StartHeavy(...)`;
- сразу возвращает `202 Accepted`.

HTTP handler не должен:

- сам публиковать в RabbitMQ;
- сам собирать отчет;
- сам читать/писать файлы;
- сам реализовывать retry;
- сам заниматься delivery.

### 2. Prepare stage

Prepare worker:

- читает `prepare` job из очереди;
- собирает данные из нужных источников;
- формирует итоговый JSON;
- сохраняет JSON в файл;
- создает `delivery` job.

### 3. Delivery stage

Delivery worker:

- читает `delivery` job из очереди;
- открывает ранее сохраненный файл;
- читает его содержимое;
- отправляет содержимое во внешний сервис;
- завершает цепочку.

---

## Что должен делать `prepare`

`prepare` должен делать только это:

1. прочитать входной payload job;
2. сходить в источники данных;
3. агрегировать данные;
4. собрать большой JSON;
5. сохранить JSON в файл;
6. создать `delivery` job.

`prepare` не должен:

- отправлять финальный JSON во внешний сервис;
- тащить большой payload в RabbitMQ;
- возвращать результат напрямую в HTTP response.

---

## Что должен делать `delivery`

`delivery` должен делать только это:

1. получить `delivery` job;
2. достать путь к файлу из payload или из родительской job;
3. открыть файл;
4. прочитать JSON;
5. отправить JSON во внешний сервис;
6. при успехе завершить job;
7. при успехе удалить временный файл, если это допустимо политикой хранения.

`delivery` не должен:

- заново собирать данные;
- заново строить JSON;
- зависеть от RabbitMQ payload как от источника большого результата.

---

## Реальный пример, найденный в `lk.sps38.pro`

В sibling-проекте найден реальный endpoint:

- `GET /api/worksheets/export`

Контроллер:

- `ShiftController::ExportShifts`

Что видно по коду:

- обязательные query params:
  - `date_from`
  - `date_to`
- опциональные query params:
  - `location_code_1c`
  - `employee_code_1c`

Но сам контроллер только проксирует вызов в сервис.

Реальная бизнес-логика находится в:

- `ShiftOldService::exportShifts(...)`

А данные оттуда собираются через:

- `ShiftOldStorage`

Причем storage использует:

- `getPdo('db_old')`

Это важно: боевой `prepare` для выгрузки worksheets, скорее всего, должен уметь ходить не только во внешний HTTP endpoint, но и в MariaDB `lk`.

---

## Рекомендуемая структура каталогов

Сейчас проект уже имеет общий infrastructure-каркас:

- `engine`
- `gateway`
- `queue`
- `storage`
- `repository`

Новые heavy-кейсы лучше разделять не по транспортам, а по бизнес-областям.

### Рекомендуемый принцип

Разделять по доменам и use case'ам:

```text
internal/
  worksheets/
    users_reports/
    vehicle/
```

И внутри каждого use case держать рядом:

- request DTO
- dedupe builder
- prepare logic
- delivery logic
- external clients

### Практическая рекомендуемая структура

```text
internal/
  worksheets/
    users_reports/
      request.go
      dedupe.go
      prepare.go
      delivery.go
      payloads.go
      client.go
    vehicle/
      request.go
      dedupe.go
      prepare.go
      delivery.go
      payloads.go
      client.go
```

### Почему так лучше

Потому что тогда одна бизнес-фича лежит в одном месте:

- `worksheets/users_reports`
- `worksheets/vehicle`

А не размазана по 10 пакетам без локальности.

Инфраструктура при этом остается общей:

- `engine`
- `storage`
- `queue`
- `repository/postgres`
- `telemetry`

---

## Как добавлять новый heavy endpoint

Ниже практический порядок.

### Шаг 1. Описать request DTO

Нужно завести request struct с валидацией.

Пример:

```go
type Request struct {
    DateFrom       string  `json:"date_from" validate:"required,datetime=2006-01-02"`
    DateTo         string  `json:"date_to" validate:"required,datetime=2006-01-02"`
    LocationCode1C *string `json:"location_code_1c,omitempty"`
    EmployeeCode1C *string `json:"employee_code_1c,omitempty"`
}
```

### Шаг 2. Построить `dedupe_key`

`dedupe_key` должен строиться из бизнес-параметров запроса.

Пример:

```go
func BuildDedupe(req Request) string {
    return fmt.Sprintf(
        "worksheets_export:%s:%s:%s:%s",
        req.DateFrom,
        req.DateTo,
        nullable(req.LocationCode1C),
        nullable(req.EmployeeCode1C),
    )
}
```

Важно:

- в `dedupe_key` должны входить только параметры, которые реально определяют уникальность выгрузки

### Шаг 3. Зарегистрировать heavy route

HTTP registration должна использовать `gateway.RegisterHigh(...)`.

Пример:

```go
gateway.RegisterHigh(router, gateway.HighOptions[Request]{
    Method:    http.MethodPost,
    Path:      "/integration/api/v1/worksheets/users-reports/export",
    Source:    "integration_api",
    Type:      "worksheets_users_reports_export",
    Direction: enginejob.DirectionInbound,
    BuildDedupe: func(req Request) (string, error) {
        return BuildDedupe(req), nil
    },
})
```

Handler не должен писать tracing, retry, outbox или RabbitMQ вручную.

### Шаг 4. Реализовать `prepare`

Нужен processor/use case, который:

1. читает job payload;
2. собирает данные;
3. формирует JSON;
4. сохраняет JSON в файл;
5. создает `delivery` job.

Если данных много:

- нормально обращаться к нескольким storage/client слоям;
- нормально агрегировать по частям;
- нормально сериализовать результат только в конце.

### Шаг 5. Определить payload для `delivery`

В delivery payload надо передавать только то, что нужно для чтения и отправки файла.

Минимально:

```go
type DeliveryPayload struct {
    PrepareJobID string `json:"prepare_job_id"`
    TempPath     string `json:"temp_path"`
}
```

### Шаг 6. Реализовать `delivery`

`delivery` должен:

1. открыть `TempPath`;
2. прочитать JSON;
3. отправить его во внешний сервис;
4. обновить статус job;
5. удалить временный файл при успехе.

Если внешний сервис требует:

- HTTP `POST`
- auth header
- retryable statuses

это должно жить в `delivery`, а не в `prepare`.

---

## Как разделять `prepare` и `delivery` ответственность

### Правильное разделение

`prepare`:

- compute
- aggregation
- file write

`delivery`:

- file read
- outbound send

### Неправильное разделение

Плохо:

- `prepare` и собирает, и сразу отправляет
- `delivery` заново собирает данные
- большой JSON кладется в RabbitMQ

---

## Какие env нужны

### Для upstream HTTP API

Если `prepare` использует внешний HTTP:

```env
PRODUCT_API_BASE_URL=
PRODUCT_API_BEARER_TOKEN=
PRODUCT_API_TIMEOUT=60s
PRODUCT_API_INSECURE_SKIP_VERIFY=false
```

### Для `lk` MariaDB

Если `prepare` должен собирать данные напрямую из `lk` MariaDB, добавлены отдельные переменные:

```env
LK_MARIADB_HOST=
LK_MARIADB_PORT=3306
LK_MARIADB_DATABASE=
LK_MARIADB_USER=
LK_MARIADB_PASSWORD=
```

Почему отдельные:

- чтобы не путать их с текущими общими `MARIADB_*`
- чтобы явно обозначить, что это datasource именно `lk`

### Что важно

Если боевой worksheets export реально живет на `db_old`, то лучше делать для него отдельный Go storage/client слой, а не смешивать это с текущим product HTTP client.

---

## Что уже делает инфраструктура автоматически

Автор heavy endpoint **не должен** вручную писать:

- `request_id`
- `correlation_id`
- OpenTelemetry trace spans
- RabbitMQ publish
- outbox persistence
- consumer wiring
- retry/backoff scheduling
- dedupe reuse
- idempotency lookup

Это уже делает платформа.

То есть автор use case должен сфокусироваться только на:

- DTO
- dedupe builder
- prepare logic
- delivery logic
- внешнем клиенте/хранилище данных

---

## Рекомендуемый следующий рефакторинг проекта

Чтобы тяжелые кейсы не росли хаотично, рекомендую двигаться к такой структуре:

```text
internal/
  worksheets/
    users_reports/
      request.go
      prepare.go
      delivery.go
      payloads.go
      dedupe.go
      client.go
    vehicle/
      request.go
      prepare.go
      delivery.go
      payloads.go
      dedupe.go
      client.go
  engine/
  queue/
  storage/
  repository/
  telemetry/
```

Это даст:

- понятную локальность кода
- простое добавление новых heavy use case'ов
- меньше риска смешать `worksheets/users_reports` и `worksheets/vehicle`

---

## Короткое правило

Если формулировать одной строкой:

```text
prepare = собрать данные и записать JSON в файл
delivery = открыть файл и отправить его содержимое во внешний сервис
```

Это и есть базовая модель heavy-запросов в этом проекте.
