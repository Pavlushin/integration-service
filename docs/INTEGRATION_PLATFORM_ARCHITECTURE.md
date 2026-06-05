# Integration Platform Architecture

## Идея

Сервис не привязан к 1С.

Это универсальная платформа для обработки интеграций, тяжелых и легких запросов, обмена данными между системами и выполнения фоновых задач.

## Основные концепции

### Light Request

Быстрые синхронные запросы:

- получение справочников;
- получение статусов;
- небольшие выборки данных.

Схема:

Request -> Handler -> Response

### Heavy Request

Тяжелые асинхронные запросы:

- месячные отчеты;
- массовые выгрузки;
- генерация документов;
- длительные расчеты.

Схема:

Request -> Job -> RabbitMQ -> Worker -> Result -> Callback

## Архитектура

cmd/
- api
- worker

internal/
- config
- transport/http
- queue/rabbitmq
- storage
- engine
  - light
  - heavy
  - registry
  - workflow
  - job
- handlers
  - onec
  - reports
  - employees
  - tickets
  - notifications
- repository

## Engine

Engine отвечает за:
- создание Job;
- публикацию в RabbitMQ;
- retry;
- dedupe;
- callback;
- workflow.

Engine не зависит от конкретной интеграции.

## RabbitMQ

Очереди:
- integration.prepare
- integration.send
- integration.retry
- integration.dlq

В очередь передается только job_id.

## Jobs

Таблица integration_jobs:

- id
- parent_id
- type
- status
- payload_json
- result_path
- attempts
- last_error
- created_at
- started_at
- finished_at

Статусы:
- pending
- processing
- retrying
- done
- failed

## Storage

Результаты тяжелых задач сохраняются отдельно.

Например:

/storage/reports/{job_id}.json

В БД хранится только путь к результату.

## Dedupe

Для каждой тяжелой задачи формируется уникальный ключ.

Если активная задача уже существует, новая не создается.

## Retry

Повторяем выполнение при:
- 500
- 502
- 503
- 504
- timeout
- connection refused

После превышения лимита попыток:
- failed
- либо DLQ

## Callback

После завершения Heavy Request результат может быть автоматически отправлен во внешнюю систему.

## Цель

Создать единый механизм обработки интеграций и тяжелых задач для любых сервисов компании.
