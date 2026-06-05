# Integration Platform Overview

## Общее описание

Integration Platform — универсальная платформа для обработки интеграций, легких и тяжелых запросов.

## Типы запросов

### Light Request

Синхронные операции:

- справочники
- статусы
- небольшие выборки

Схема:

Client -> HTTP -> Handler -> Response

### Heavy Request

Асинхронные операции:

- отчеты
- массовые выгрузки
- генерация документов
- длительные расчеты

Схема:

Client -> Request -> Job -> RabbitMQ -> Worker -> Result -> Callback

## Job Engine

Отвечает за:

- создание Job
- статусы
- retry
- dedupe
- callback
- workflow

## RabbitMQ

Очереди:

- integration.prepare
- integration.send
- integration.retry
- integration.dlq

В RabbitMQ передается только job_id.

## Workflow

Простой:

Prepare -> Send

Расширенный:

Prepare -> Validate -> Transform -> Send

## Retry

Повторяем при:

- 500
- 502
- 503
- 504
- timeout
- connection refused

## Dedupe

Не позволяет создавать одинаковые активные задачи.

## Callback

После завершения задачи результат отправляется инициатору автоматически.

## Преимущества

Технические:

- масштабируемость
- отказоустойчивость
- переиспользуемость
- единая архитектура

Бизнес:

- быстрое подключение новых интеграций
- снижение стоимости поддержки
- уменьшение количества ошибок

## Будущее развитие

- Scheduler
- Outbox Pattern
- Webhook Engine
- Event Bus
- Circuit Breaker
- Audit Log
- Replay Failed Job
