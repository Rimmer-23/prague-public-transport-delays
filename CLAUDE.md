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
- RSS со сбоями и ограничениями движения — для сопоставления пиков задержек с происшествиями.
- Опрос API — не чаще раза в 10–30 с (лимиты).
- Сбор данных запустить как можно раньше: для «нормы» по линиям и батч-аналитики нужна история за дни/недели.

## Архитектура (целевая)

```
Golemio API ──► producer (Python) ──► Kafka: pid.vehicle_positions ──► Spark Structured Streaming
PID RSS     ──► producer (Python) ──► Kafka: pid.disruptions               │  окна 15 мин по линиям/остановкам
                                                                            │  аномалии → Kafka: pid.alerts → Telegram-бот
                                                                            ├► PostgreSQL (агрегаты) ──► Grafana
                                                                            └► Parquet по датам (сырые данные)
                                    Spark batch (раз в сутки) ◄── Parquet: рейтинг линий, часы пик, связь со сбоями
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
  - Spark: `apache/spark:4.2.1-java21-python3` (local mode в одном контейнере), коннектор `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1`.
    Код джоб — `spark/jobs/` (монтируется в `/opt/spark/jobs`), кэш скачанных jar — `.cache/ivy`.
- Зависимости Python — в виртуальном окружении `.venv` (в git не попадает).

## План (этапы)

1. [x] Токен Golemio; скрипт `explore`: забрать позиции машин и вывести в консоль — разобраться в структуре данных
2. [x] Docker Compose с Kafka (KRaft); продюсер пишет позиции в `pid.vehicle_positions`; читаем топик консольным консьюмером
3. [x] Spark Structured Streaming: парсинг JSON, средняя задержка по линиям в окнах 15 мин; вывод в консоль
4. [ ] Хранение: сырые события → Parquet по датам, агрегаты → PostgreSQL
5. [ ] Grafana: задержки по линиям, карта
6. [ ] Аномалии и алерты: `pid.alerts` → Telegram-бот (подписка на линию)
7. [ ] Справочник GTFS (названия линий/остановок), RSS сбоев → `pid.disruptions`
8. [ ] Ежедневная батч-джоба: проблемные линии, часы пик, трамваи vs автобусы, корреляция со сбоями
9. [ ] README для портфолио: проблема, архитектура (схема), выводы на реальных данных, как запустить

## Правила кода

- Секреты — только в `.env`; `.env.example` держать актуальным.
- Каждая часть запускается одной командой; команды записывать в раздел «Команды» ниже по мере появления.
- Спорные решения (формат сообщений, ключ партиционирования, размер окна, watermark) — коротко фиксировать
  в `docs/decisions.md` с причиной: это пригодится и для собеседования.

## Команды

- Окружение: `py -3.14 -m venv .venv` и `.venv/Scripts/python -m pip install -r requirements.txt`
- Снимок позиций машин (структура данных): `.venv/Scripts/python scripts/explore.py` → `data/explore/`
- Kafka: `docker compose up -d` / `docker compose down` (данные в volume `kafka-data` сохраняются; `down -v` — удалить и их)
- Создать топик: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --topic pid.vehicle_positions --partitions 3 --replication-factor 1`
- Список / описание топиков: `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list` (или `--describe --topic <name>`)
- Читать топик: `docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true`
- Писать в топик руками (`ключ:значение`): `docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions --reader-property parse.key=true --reader-property key.separator=: --reader-property ignore.error=true` (без `ignore.error` строка без `:`, даже пустая, роняет продюсер)
- Продюсер позиций (опрос API → Kafka, Ctrl+C — стоп): `.venv/Scripts/python producers/vehicle_positions.py`
- Сколько сообщений в партициях (последний offset): `docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic pid.vehicle_positions`
- Spark-джоба (пример — чтение топика в консоль, Ctrl+C — стоп): `docker exec -it spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 --conf spark.jars.ivy=/opt/spark/ivy /opt/spark/jobs/read_positions.py`
- Средняя задержка по линиям в окнах 15 мин (консоль): та же команда с `/opt/spark/jobs/delays_by_line.py`
- Checkpoint джобы — `checkpoints/<job>` (в git не попадает). Удалить папку = джоба начнёт заново с начала топика.
- Spark UI работающей джобы: http://localhost:4040
- В Git Bash перед `docker exec` с путями `/opt/...` нужно `export MSYS_NO_PATHCONV=1` (иначе Git Bash портит путь); в PowerShell не нужно.
