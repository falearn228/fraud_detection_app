# Fraud Detection App

> Система обнаружения мошеннических транзакций в реальном времени с использованием Kafka Streams, PostgreSQL и Change Data Capture (CDC)

## Оглавление

- [Описание](#описание)
- [Архитектура](#архитектура)
- [Технологии](#технологии)
- [Быстрый старт](#быстрый-старт)
- [API](#api)
- [SSL/TLS Конфигурация](#ssltls-конфигурация)
- [Переменные окружения](#переменные-окружения)
- [Правила детекции мошенничества](#правила-детекции-мошенничества)

---

## Описание

Приложение реализует два механизма обнаружения мошенничества:

1. **Простой анализ** - проверка каждой транзакции по базовым правилам
2. **Stream анализ** - оконный анализ частоты транзакций пользователя (временные окна 5 минут)

Система работает в режиме реального времени, используя Debezium для CDC и Kafka для обработки потоков событий.

## Архитектура

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Client    │────▶│  Go Service  │────▶│ PostgreSQL  │
└─────────────┘     └──────────────┘     └─────────────┘
                            │                    │
                            │                    ▼
                            │             ┌─────────────┐
                            │             │  Debezium   │
                            │             │     CDC     │
                            │             └─────────────┘
                            │                    │
                            ▼                    ▼
                     ┌──────────────────────────────┐
                     │       Kafka Broker           │
                     │  (SSL/TLS encrypted)         │
                     └──────────────────────────────┘
                            │           │
               ┌────────────┘           └────────────┐
               ▼                                     ▼
    ┌─────────────────────┐              ┌─────────────────────┐
    │ Fraud Detection     │              │ Stream Processing   │
    │ (Simple Rules)      │              │ (Window Analysis)   │
    └─────────────────────┘              └─────────────────────┘
               │                                     │
               └─────────────┬───────────────────────┘
                             ▼
                     ┌───────────────┐
                     │ Fraud Results │
                     │    Topics     │
                     └───────────────┘
```

### Компоненты:

- **Go Service** - REST API для приема транзакций и их анализа
- **PostgreSQL** - хранение транзакций
- **Kafka** - брокер сообщений с SSL шифрованием
- **Debezium** - CDC коннектор для отслеживания изменений в PostgreSQL
- **Fraud Detection Service** - простой анализ транзакций по правилам
- **Fraud Streams Service** - оконный анализ частоты транзакций

## Технологии

- **Go 1.21+** - основной язык разработки
- **Kafka 3.x** - обработка потоков событий
- **PostgreSQL 15** - база данных
- **Debezium 2.x** - Change Data Capture
- **Docker & Docker Compose** - контейнеризация
- **Gin** - HTTP фреймворк
- **Sarama** - Kafka клиент для Go

## Быстрый старт

### Требования

- Docker 20.10+
- Docker Compose 2.0+

### Запуск

```bash
# Клонировать репозиторий
git clone <repository-url>
cd fraud_detection_app

# Запустить все сервисы
docker-compose up --build

# В фоновом режиме
docker-compose up -d --build
```

### Проверка работоспособности

```bash
# Создать тестовую транзакцию
curl -X POST http://localhost:8080/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 150.0,
    "currency": "USD",
    "userId": 123
  }'

# Проверить логи
docker-compose logs -f fraud-detection-app

# Проверить Kafka топики
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud-detection-results \
  --from-beginning
```

## API

### Создание транзакции

**Endpoint:** `POST /transactions`

**Request Body:**
```json
{
  "amount": 150.0,
  "currency": "USD",
  "userId": 123
}
```

**Response:** `201 Created`
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "amount": 150.0,
  "currency": "USD",
  "userId": 123,
  "status": "NEW",
  "timestamp": "2025-10-01T12:00:00Z"
}
```

### Статусы транзакций

- `NEW` - новая транзакция
- `SUSPICIOUS` - подозрительная транзакция (обнаружено мошенничество)

## SSL/TLS Конфигурация

Приложение использует SSL/TLS для безопасного соединения с Kafka.

### Структура сертификатов

```
ssl/
├── ca-cert.pem          # Корневой сертификат CA
├── ca-key.pem           # Приватный ключ CA
├── kafka-cert.pem       # Сертификат Kafka брокера
├── kafka-key-nopass.pem # Приватный ключ без пароля (для Go)
├── kafka.jks            # Java KeyStore для Kafka
├── truststore.jks       # TrustStore с CA сертификатом
└── ssl_creds            # Пароль для keystore (changeit)
```

### Генерация сертификатов

```bash
# 1. Создать CA
openssl req -x509 -newkey rsa:4096 -keyout ca-key.pem -out ca-cert.pem -days 365 \
  -subj "/CN=Kafka CA" -passout pass:changeit

# 2. Создать truststore
keytool -keystore truststore.jks -alias CARoot -import -file ca-cert.pem \
  -storepass changeit -keypass changeit -noprompt

# 3. Создать ключ для Kafka с SAN
openssl req -new -newkey rsa:4096 -keyout kafka-key.pem -out kafka-csr.pem \
  -subj "/CN=kafka" -passout pass:changeit \
  -addext "subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1"

# 4. Подписать сертификат
echo "subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1" > san.ext

openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in kafka-csr.pem \
  -out kafka-cert.pem -days 365 -CAcreateserial -passin pass:changeit \
  -extfile san.ext

# 5. Создать PKCS12 с полной цепочкой
openssl pkcs12 -export -in kafka-cert.pem -inkey kafka-key.pem \
  -out kafka.p12 -name kafka -CAfile ca-cert.pem -caname CARoot \
  -chain -password pass:changeit -passin pass:changeit

# 6. Конвертировать в JKS
keytool -importkeystore -deststorepass changeit -destkeypass changeit \
  -destkeystore kafka.jks -srckeystore kafka.p12 -srcstoretype PKCS12 \
  -srcstorepass changeit -alias kafka

# 7. Убедиться, что CA есть в keystore
keytool -import -trustcacerts -alias CARoot -file ca-cert.pem \
  -keystore kafka.jks -storepass changeit -noprompt

# 8. Создать версию ключа без пароля для Go
openssl rsa -in kafka-key.pem -out kafka-key-nopass.pem -passin pass:changeit

# 9. Создать файл с паролем
echo "changeit" > ssl_creds
```

## Переменные окружения

### Kafka

| Переменная | Описание | По умолчанию |
|-----------|----------|--------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Адреса Kafka брокеров | `kafka:9092` |
| `KAFKA_SECURITY_PROTOCOL` | Протокол безопасности | `SSL` |
| `KAFKA_SSL_CA_LOCATION` | Путь к CA сертификату | `/app/ssl/ca-cert.pem` |
| `KAFKA_SSL_CERT_LOCATION` | Путь к клиентскому сертификату | `/app/ssl/kafka-cert.pem` |
| `KAFKA_SSL_KEY_LOCATION` | Путь к приватному ключу | `/app/ssl/kafka-key-nopass.pem` |
| `DEBEZIUM_TRANSACTION_TOPIC` | Топик Debezium | `fraud-detection.public.transaction` |
| `FRAUD_RESULTS_TOPIC` | Топик результатов простого анализа | `fraud-detection-results` |
| `FRAUD_STREAM_RESULTS_TOPIC` | Топик результатов stream анализа | `fraud-detection-stream-results` |

### PostgreSQL

| Переменная | Описание | По умолчанию |
|-----------|----------|--------------|
| `POSTGRES_HOST` | Хост базы данных | `postgres` |
| `POSTGRES_PORT` | Порт базы данных | `5432` |
| `POSTGRES_USER` | Пользователь | `debezium` |
| `POSTGRES_PASSWORD` | Пароль | `dbz` |
| `POSTGRES_DB` | Имя базы данных | `transactions_db` |
| `POSTGRES_SSL_MODE` | Режим SSL | `disable` |

## Правила детекции мошенничества

### Простой анализ (FraudDetectionService)

Анализирует каждую транзакцию индивидуально:

- **Высокая сумма** - транзакция > 10000: +0.8 к риску
- **Новый статус** - статус = NEW: +0.2 к риску
- **Порог мошенничества** - риск > 0.7 = МОШЕННИЧЕСТВО

### Stream анализ (FraudStreamsService)

Анализирует паттерны транзакций пользователя:

- **Окно анализа** - 5 минут
- **Условия срабатывания детектора**:
  - MaxAmount > 5000 ИЛИ
  - 3+ транзакции со средней суммой > 2000
- **Расчет риска** - учитывается частота транзакций (×0.3) и максимальная сумма (÷10000)
- **Очистка окон** - каждые 30 секунд

### Результаты анализа

Результаты отправляются в Kafka топики:

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "is_fraudulent": true,
  "risk_score": 0.9,
  "reason": "High transaction amount; New transaction status"
}
```

## Мониторинг

### Kafka топики

```bash
# Просмотр результатов простого анализа
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud-detection-results \
  --from-beginning

# Просмотр результатов stream анализа
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud-detection-stream-results \
  --from-beginning

# Просмотр CDC событий от Debezium
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud-detection.public.transaction \
  --from-beginning
```

### Логи сервисов

```bash
# Все логи
docker-compose logs -f

# Конкретный сервис
docker-compose logs -f fraud-detection-app
docker-compose logs -f kafka
docker-compose logs -f postgres
```

## Разработка

### Структура проекта

```
fraud_detection_app/
├── cmd/
│   └── main.go                    # Точка входа приложения
├── internal/
│   ├── config/                    # Конфигурация
│   │   ├── kafka.go               # Конфигурация Kafka
│   │   └── database.go            # Конфигурация БД
│   ├── controllers/               # HTTP контроллеры
│   │   └── transaction_controller.go
│   ├── kafka/                     # Kafka клиенты
│   │   └── producer.go
│   ├── models/                    # Модели данных
│   │   └── transaction.go
│   ├── repositories/              # Слой работы с БД
│   │   ├── transaction_repository.go
│   │   └── postgres_transaction_repository.go
│   └── services/                  # Бизнес-логика
│       ├── fraud_detection_service.go
│       └── fraud_streams_service.go
├── ssl/                           # SSL сертификаты
├── docker-compose.yml             # Docker Compose конфигурация
├── Dockerfile                     # Dockerfile приложения
├── go.mod                         # Go зависимости
└── README.md
```

### Сборка и запуск локально

```bash
# Установить зависимости
go mod download

# Запустить зависимости через Docker
docker-compose up -d postgres kafka

# Запустить приложение локально
go run cmd/main.go
```
