# prague-public-transport-delays

Учебный и портфолио-проект: мониторинг задержек общественного транспорта Праги (PID) в реальном времени
на **Apache Kafka** и **Apache Spark** (Structured Streaming + батч).

**Проблема:** официальные приложения показывают задержку одного рейса, но никто не видит общей картины —
какие линии, участки и часы систематически опаздывают и когда задержки резко растут.

## Как мы работаем (важно)

- Главная цель автора — **разобраться в Kafka и Spark**, вторая — проект для портфолио.
- Пишем код **по шагам, с объяснениями**: каждый шаг — небольшой, запускается и даёт видимый результат.
  Перед кодом коротко объяснять, *что* делаем и *зачем* (какое понятие Kafka/Spark тут работает),
  после — как проверить, что работает. Не генерировать сразу весь проект.
- Новые понятия (топик, партиция, consumer group, offset, watermark, окно, checkpoint, output mode…)
  объяснять при первом появлении, простыми словами, на примере данных этого проекта.
- Не делать за автора шаги, которые он хочет сделать сам; если неясно — спросить.
- Общение — на русском. Код, имена, коммиты — на английском.

## Данные (PID / Golemio)

- **Golemio API** — городская дата-платформа Праги. Нужен бесплатный токен (api.golemio.cz),
  передаётся в заголовке `X-Access-Token`. Токен — только в `.env` (в git не попадает), в репозитории — `.env.example`.
- Позиции машин и **задержка уже посчитаны PID** — Spark агрегирует их, а не вычисляет задержку из расписания.
- Статическое расписание GTFS (pid.cz/opendata) — справочник линий и остановок, обновляется ежедневно ~4:00–4:30.
- RSS со сбоями и ограничениями движения — для сопоставления пиков задержек с происшествиями:
  `https://pid.cz/feed/rss-mimoradnosti/` (внеплановые) и `https://pid.cz/feed/rss-vyluky/` (плановые). Кроме стандартных
  полей RSS в них есть `<lines>`, `<dateFrom>`, `<dateTo>` (секунды epoch; пусто = «до отмены»).
- Опрос API — не чаще раза в 10–30 с (лимиты).
- Сбор данных запустить как можно раньше: для «нормы» по линиям и батч-аналитики нужна история за дни/недели.

## Архитектура (целевая)

```
Golemio API ──► producer (Python) ──► Kafka: pid.vehicle_positions ──► Spark Structured Streaming (5 запросов)
PID RSS     ──► producer (Python) ──► Kafka: pid.disruptions ─────────►   │  окна 15 мин по линиям → PostgreSQL
GTFS (zip)  ──► scripts/load_gtfs.py ──► PostgreSQL + data/gtfs            │  последняя позиция рейса → PostgreSQL (карта)
                                                                            │  сбои → PostgreSQL
                                                                            │  аномалии → Kafka: pid.alerts → Telegram-бот
                                                                            └► Parquet по датам (сырые данные)
              Spark batch (раз в сутки) ◄── Parquet + GTFS + pid.disruptions: рейтинг линий, часы пик,
                                             проблемные участки, связь со сбоями, «норма» линий → PostgreSQL ──► Grafana
```

## Стек и окружение

- Windows 11, Docker Desktop (WSL2), Docker Compose v5. Kafka, Spark, PostgreSQL, Grafana — **только в Docker**,
  на систему не ставим.
- Kafka в режиме **KRaft** (без ZooKeeper).
- Python 3.14 локально — для продюсеров и скриптов. Spark-джобы (PySpark) выполняются в контейнере Spark,
  поэтому версия Python там своя. Если библиотека не поддерживает 3.14 — ставить через venv
  нужной версии или запускать продюсер тоже в Docker.
- Версии образов фиксировать явно (без `latest`) и записывать сюда, когда выберем:
  - Kafka: `apache/kafka:4.3.2` (KRaft, один узел broker+controller). Из Windows — `localhost:9092`, из контейнеров — `kafka:19092`.
    Топики создаёт одноразовый контейнер `kafka-init` при `docker compose up`: `pid.vehicle_positions` (3 партиции, ключ `trip_id`),
    `pid.disruptions` (1 партиция, **compacted**, ключ — id сбоя), `pid.alerts` (1 партиция, ключ — линия).
  - PostgreSQL: `postgres:18.6-alpine`, контейнер `pid-postgres` (имя `postgres` занято другим проектом). Из Windows — `localhost:5432`,
    из контейнеров — `postgres:5432`. База/пользователь/пароль — `POSTGRES_*` в `.env`. Схема — `postgres/init/*.sql`
    (выполняется только при первом создании volume `postgres-data`; все файлы можно безопасно выполнять повторно).
    Из Windows подключаться к `127.0.0.1`, не к `localhost`: порт опубликован только по IPv4, а `localhost` сначала
    пробуется как IPv6 (`::1`) — psycopg на этом зависал.
  - Grafana: `grafana/grafana:13.2.5`, контейнер `pid-grafana`, http://localhost:3001 (порт 3000 занят другим проектом).
    Дашборды видны без входа (анонимный Viewer), редактирование — `admin` / `GRAFANA_ADMIN_PASSWORD` из `.env`.
    Источник данных и дашборды — файлами: `grafana/provisioning/`, `grafana/dashboards/*.json`
    (`line_delays` — задержки по линиям, алерты, сбои; `live_map` — карта; `daily_analytics` — результаты батч-джобы). В базу Grafana ходит
    под read-only ролью `grafana_reader` (`postgres/init/02_grafana_reader.sql`, пароль — `GRAFANA_DB_PASSWORD`).
  - Spark: свой образ `pid-spark:4.2.1` = `apache/spark:4.2.1-java21-python3` + `psycopg[binary]==3.3.8` (`spark/Dockerfile`;
    после правки Dockerfile — `docker compose up -d --build spark`). Local mode в одном контейнере, коннектор `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1`.
    Код джоб — `spark/jobs/` (монтируется в `/opt/spark/jobs`), кэш скачанных jar — `.cache/ivy`,
    данные — `data/` (монтируется в `/opt/spark/data`). `spark.sql.shuffle.partitions=8` (см. `docs/decisions.md`).
  - Python-сервисы в Docker (профиль `pipeline`): образ `pid-python:3.14` = `python:3.14.7-slim` + `requirements.txt`
    (`Dockerfile` в корне); код монтируется, после правки скрипта — `docker compose --profile pipeline restart <сервис>`.
- Зависимости Python — в виртуальном окружении `.venv` (в git не попадает).

## План (этапы)

1. [x] Токен Golemio; скрипт `explore`: забрать позиции машин и вывести в консоль — разобраться в структуре данных
2. [x] Docker Compose с Kafka (KRaft); продюсер пишет позиции в `pid.vehicle_positions`; читаем топик консольным консьюмером
3. [x] Spark Structured Streaming: парсинг JSON, средняя задержка по линиям в окнах 15 мин; вывод в консоль
4. [x] Хранение: сырые события → Parquet по датам, агрегаты → PostgreSQL
5. [x] Grafana: задержки по линиям (5a), карта (5b)
6. [x] Аномалии и алерты: `pid.alerts` → Telegram-бот (подписка на линию). Не проверено с настоящим токеном:
       нужен `TELEGRAM_BOT_TOKEN` от @BotFather в `.env`; без него бот печатает алерты в консоль
7. [x] Справочник GTFS (названия линий/остановок), RSS сбоев → `pid.disruptions`
8. [x] Ежедневная батч-джоба: проблемные линии, часы пик, трамваи vs автобусы, корреляция со сбоями
9. [x] README для портфолио: проблема, архитектура (схема), выводы на реальных данных, как запустить.
       Выводы пока предварительные (данных — несколько часов); обновить цифры и добавить скриншоты через 1–2 недели сбора

## Правила кода

- Секреты — только в `.env`; `.env.example` держать актуальным.
- Каждая часть запускается одной командой; команды записывать в раздел «Команды» ниже по мере появления.
- Спорные решения (формат сообщений, ключ партиционирования, размер окна, watermark) — коротко фиксировать
  в `docs/decisions.md` с причиной: это пригодится и для собеседования.

## Команды

- Окружение: `py -3.14 -m venv .venv` и `.venv/Scripts/python -m pip install -r requirements.txt`
- Снимок позиций машин (структура данных): `.venv/Scripts/python scripts/explore.py` → `data/explore/`
- **Весь конвейер одной командой** (продюсеры, бот, GTFS, все стриминговые запросы, ежедневный батч — сервисами с автоперезапуском):
  `docker compose --profile pipeline up -d --build`. Логи: `docker compose --profile pipeline logs -f --tail 20` (или `docker logs -f pid-spark-streaming`).
  Остановить всё: `docker compose --profile pipeline down` (данные остаются). Только сервисы конвейера, инфраструктуру оставить:
  `docker compose --profile pipeline stop producer-positions producer-disruptions telegram-bot gtfs-loader spark-streaming spark-daily`.
  Spark UI стриминга — http://localhost:4041. **Пока профиль запущен, не запускать те же джобы/продюсеры вручную**: два запроса на одном
  checkpoint портят его, два продюсера удваивают данные, два бота с одним `group.id` делят алерты между собой.
- Инфраструктура без конвейера: `docker compose up -d` / `docker compose down` (данные в volume `kafka-data` сохраняются; `down -v` — удалить и их)
- Создать топик вручную (обычно не нужно — это делает `kafka-init`): `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --topic pid.vehicle_positions --partitions 3 --replication-factor 1`
- Список / описание топиков: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list` (или `--describe --topic <name>`)
- Читать топик: `docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true`
- Писать в топик руками (`ключ:значение`): `docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --reader-property parse.key=true --reader-property key.separator=: --reader-property ignore.error=true` (без `ignore.error` строка без `:`, даже пустая, роняет продюсер)
- Продюсер позиций (опрос API → Kafka, Ctrl+C — стоп): `.venv/Scripts/python producers/vehicle_positions.py`
- Сколько сообщений в партициях (последний offset): `docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions`
- Spark-джоба (пример — чтение топика в консоль, Ctrl+C — стоп): `docker exec -it spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 --conf spark.jars.ivy=/opt/spark/ivy /opt/spark/jobs/read_positions.py`
- Средняя задержка по линиям в окнах 15 мин (консоль): та же команда с `/opt/spark/jobs/delays_by_line.py`
- Архив сырых позиций Kafka → Parquet по датам (батч раз в 5 мин, Ctrl+C — стоп): та же команда с `/opt/spark/jobs/raw_to_parquet.py` → `data/raw/vehicle_positions/date=YYYY-MM-DD/`
- Проверить архив (батч-джоба, коннектор Kafka не нужен): `docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/jobs/inspect_raw.py`
- Сбросить архив: удалить **и** `data/raw/vehicle_positions`, **и** `checkpoints/raw_to_parquet` (только одно из двух = дубли или пропуски).
- Агрегаты по линиям (окна 15 мин) → PostgreSQL, таблица `line_delays_15m` (Ctrl+C — стоп): та же команда с `/opt/spark/jobs/delays_to_postgres.py`
- Последние позиции рейсов → PostgreSQL, таблица `vehicle_positions_latest` (для карты): та же команда с `/opt/spark/jobs/positions_to_postgres.py`
- Сбои Kafka → PostgreSQL, таблица `disruptions`: та же команда с `/opt/spark/jobs/disruptions_to_postgres.py`
- Поиск аномалий → таблица `line_alerts` и топик `pid.alerts`: та же команда с `/opt/spark/jobs/detect_anomalies.py`
- Все пять стриминговых запросов в одном Spark-приложении: та же команда с `/opt/spark/jobs/run_all.py`
- Ежедневная батч-джоба (по умолчанию — вчерашний день): та же команда с `/opt/spark/jobs/daily_stats.py`; параметры после имени файла:
  `--date 2026-11-10`, `--all` (все дни архива), `--explain` (показать физический план). Повторный запуск за тот же день заменяет строки, а не дублирует.
- Продюсер сбоев (RSS → Kafka, Ctrl+C — стоп): `.venv/Scripts/python producers/disruptions.py`
- Справочник GTFS (скачать, если обновился, и загрузить в PostgreSQL + `data/gtfs/`): `.venv/Scripts/python scripts/load_gtfs.py` (`--force` — скачать заново)
- Telegram-бот (Ctrl+C — стоп): `.venv/Scripts/python bot/telegram_bot.py`. Токен — `TELEGRAM_BOT_TOKEN` в `.env` (получить у @BotFather, `/newbot`);
  без токена — консольный режим. Команды в чате: `/subscribe 22`, `/subscribe all`, `/unsubscribe 22`, `/list`, `/top`.
- Читать алерты / сбои из Kafka: команда «Читать топик» с `--topic pid.alerts` или `--topic pid.disruptions`
- Где остановился бот (consumer group, committed offset, lag): `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group pid-telegram-bot`
- Применить схему к уже существующей базе (после добавления файла в `postgres/init/`): `docker exec pid-postgres sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"; done'`
- SQL-консоль PostgreSQL: `docker exec -it pid-postgres psql -U pid -d pid` (пример: `select * from line_delays_15m where trips >= 3 order by window_start desc, avg_delay_s desc limit 20;`)
- Пересчитать `line_delays_15m` с начала топика: удалить `checkpoints/delays_to_postgres` и запустить джобу (upsert перезапишет строки, дублей не будет).
- Пересчитать алерты с начала топика: `TRUNCATE line_alerts`, удалить `checkpoints/detect_anomalies`, запустить джобу (старые алерты бот пропустит по возрасту).
- Дашборды: http://localhost:3001 — «PID delays by line» (открывается сразу; `line_delays_15m`, алерты, сбои; обновление раз в 30 с),
  «PID live map» (карта машин с цветом по задержке), «PID daily analytics» (результаты батч-джобы; день выбирается сверху). Переход между ними — ссылки вверху дашборда.
- Изменить дашборд: править `grafana/dashboards/<имя>.json` (Grafana перечитывает папку раз в 30 с) или отредактировать в UI под `admin`,
  затем Export → JSON и сохранить в этот файл (сохранить provisioned-дашборд прямо из UI нельзя).
- Создать/обновить роль `grafana_reader` в уже существующей базе (или сменить её пароль): `docker exec pid-postgres psql -U pid -d pid -f /docker-entrypoint-initdb.d/02_grafana_reader.sql`
- Checkpoint джобы — `checkpoints/<job>` (в git не попадает). Удалить папку = джоба начнёт заново с начала топика.
- Spark UI работающей джобы: http://localhost:4040
- В Git Bash перед `docker exec` с путями `/opt/...` нужно `export MSYS_NO_PATHCONV=1` (иначе Git Bash портит путь); в PowerShell не нужно.
