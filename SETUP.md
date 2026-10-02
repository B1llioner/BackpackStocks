# Полная настройка BackpackStocks

## Требования и установка

Нужны Python 3.12+, Node.js 20+/npm, Git и macOS/Linux. Для ускоренного модуля нужны Rust и C/C++ build tools; без него доступны точные Python-расчёты. Репозиторий приватный, клонирование требует доступа GitHub.

```bash
git clone https://github.com/B1llioner/BackpackStocks.git
cd BackpackStocks
python3.12 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[jupiter,dev]'
npm --prefix local_clmm ci --ignore-scripts --no-audit --no-fund
cp .env.example .env
chmod 600 .env
```

Заполните `.env` в редакторе. Приложение загружает необходимые поля без исполнения shell-кода: `source .env` не нужен. Уже экспортированные переменные имеют приоритет над файлом.

## Заполнение .env

| Поле | Что указать |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Токен своего бота от `@BotFather`. |
| `TELEGRAM_ALLOWED_USER_ID` | Числовой Telegram ID своего аккаунта. Бот отвечает только ему в личном чате. |
| `BACKPACK_ED25519_SEED_BASE64` | Base64 32-байтового Ed25519 signing seed API-ключа Backpack. Это не публичный API key и не ключ Solana. Ключ должен разрешать чтение, создание и отмену RFQ. |
| `OUTBOUND_PROXY_URL` | Свой `http://user:password@host:port` или `https://...`. Учебный адрес заменить. Спецсимволы в логине/пароле должны быть URL-encoded. |
| `JUPITER_REPLAY_PROXY` | Необязательно: отдельный HTTP(S) proxy Jupiter вместо общего. |
| `JUPITER_QUOTE_WALLET` | Необязательно: публичный адрес Solana для контекста quote, подпись не нужна. |
| `JUPITER_API_KEY` | Необязательно: свой developer key для developer API/CLI. Monitor Manual использует веб-процесс сайта. |
| `JUPITER_REPLAY_WEBSITE_KEY` | Обычно не нужно: publishable config сайта скачивается скриптом установки. |

Поля `SOLANA_WALLET_KEY` и `SOLANA_KEYPAIR_BASE64` для paper/монитора оставьте пустыми. Они нужны только отдельным реальным операциям. При необходимости заполните отдельные `*_PROXY_URL` для Backpack/WS/RPC/Telegram; пустые поля используют общий маршрут. HTTP Jupiter использует `curl_cffi` с Chrome TLS/HTTP2; заданный прокси не заменяется молча прямым соединением. Jupiter replay требует HTTP(S) proxy.

Загрузите публичный конфиг Jupiter:

```bash
.venv/bin/python scripts/setup_jupiter.py --env-file .env
```

Скрипт не читает кошелёк или cookies и не отправляет сделки. Он скачивает один публичный JS, проверяет SHA-256 и сохраняет его в игнорируемый `data/jupiter`. Если файл изменился или удалён, скрипт останавливается: после проверки новой сборки обновите pin в `src/backpack_stocks/jupiter_frontend_source.json`.

## Rust-ускорение

При установленном Rust toolchain:

```bash
source .venv/bin/activate
python -m pip install 'maturin>=1.9,<2'
maturin develop --release --manifest-path rust/pool-math/Cargo.toml --features python,pyo3/extension-module
python -c 'import _pool_math; print("Rust module loaded")'
```

При смене версии Python модуль нужно пересобрать. `/monitor` показывает backend арифметики. Rust ускоряет точные пакетные расчёты; сетевой путь сохраняет `curl_cffi`, прокси и API budgets.

## Запуск Telegram-бота

Откройте личный чат своего бота и отправьте `/start`, затем из корня проекта:

```bash
.venv/bin/python scripts/restart_direct_paper.py --config examples/telegram-direct-paper-1k.toml
```

Скрипт создаёт `state/`, запускает процесс в фоне и сохраняет PID. При повторном запуске останавливает только этот paper-процесс. Не запускайте два процесса одного бота. Для запуска в текущем терминале вместо restart:

```bash
.venv/bin/backpack-stocks --config examples/telegram-direct-paper-1k.toml telegram
```

По умолчанию используется **paper**, даже если API-ключ разрешает торговлю. Монитор создаёт и отменяет проверочные `AwaitAccept` RFQ, не принимает их и не подписывает свопы. Paper FSM моделирует WEN, а монитор спредов охватывает все доступные акции.

| Команда | Что показывает |
|---|---|
| `/prices` | Все акции в нескольких сообщениях: Backpack/Jupiter, фактический объём до 1000 USDC, минимум выхода, RFQ, время обеих цен и период обновления. |
| `/prices PTN` | Те же данные одной акции. |
| `/spreads` | Только положительные свежие спреды после расходов. |
| `/spread PTN` | Расчёт акции и приоритет её обновления. |
| `/monitor` | Покрытие, ошибки, скорость, количество quotes, backend арифметики. |
| `/limits` | API budgets, очередь, 429 и cooldown. |
| `/account` | Авторизация и реальные балансы Backpack. |
| `/status` | Режим и состояние FSM. |
| `/stop`, `/resume` | Управление FSM; монитор продолжает обновлять цены. |

## Актуальность и параметры

В профиле 1k целевой объём — **до 1000 USDC**. Количество округляется по min/max/step RFQ-сессии, поэтому сумма может быть меньше. Расчёт учитывает реальную цену RFQ, комиссию вывода, своп доставленного количества, minimum output после slippage, комиссии и gas.

Доступность RFQ кешируется на 60 секунд. Для спреда цена RFQ и DEX должны быть свежими — максимум 15 секунд. Кеш доступности не продлевает цену. Все акции проверяются по очереди: `quote_refresh_s=10` — минимальная цель повторения, не обещание обновить весь список за 10 секунд. `/prices` показывает фактический медианный период и время по Москве. Старые цены отмечены как устаревшие и не участвуют в спреде. Отсутствие положительных спредов не означает отсутствие котировок.

`RFQ доступна` означает недавно наблюдавшуюся котировку на проверенный размер и сторону; это не гарантия исполнения новой заявки. При отказе, закрытой сессии или непроверенном RFQ спред исключается. Недостаточный USDC для покупки или недостаточное количество акций для продажи дают отказ по балансу. Проверочная RFQ отменена; реальная сделка требует новой заявки и подтверждения исполнения.

Для своего объёма скопируйте TOML и одновременно измените `monitor.notional_usdc`, `policy.max_cost_usdc` и `paper_notional_usdc`. Назначьте отдельную `database`, особенно если старый цикл ещё открыт. Секретов в TOML быть не должно.

## Логи и диагностика

```bash
tail -f state/runtime-1k.jsonl
tail -n 80 state/telegram.log
.venv/bin/python scripts/inspect_direct_runtime.py
.venv/bin/python scripts/check_rfq_validator.py --symbol WEN --quantity 1
```

Последняя команда только проверяет и отменяет RFQ; `--side Ask` проверяет продажу. Её журнал — `state/rfq-diagnostic.jsonl`. Основной срез — `state/direct-monitor.json`.

События: `quote_complete` (обе цены, выход и задержка), `rfq_check`, `rfq_rejected`, `rfq_cancel_failed`, `quote_error`, `http_rejected`, `http_backoff`, `heartbeat`, `telegram_send_error`. JSON-журнал ротируется по 4 MB; credentials и сырые ответы исключены. Для мониторинга достаточно одного Telegram-процесса, фоновые задачи уже встроены.

Если нет свежих цен, смотрите `/monitor`, `/limits`, RFQ-сессию, 401/403, proxy, 429/cooldown и ошибки маршрута. `INSUFFICIENT_FUNDS` — отказ Backpack по балансу. `jupiter_http_error` содержит HTTP status; `jupiter_transport_error` — сбой proxy/транспорта. При ошибке старый расчёт перестаёт быть актуальным, повтор идёт с паузой.

## CLI Jupiter без исполнения

```bash
.venv/bin/python scripts/jupiter_flow.py quote \
  --env-file .env --transport curl_cffi --profile manual \
  --input-mint PTNzAfFAB4LvoUQEUUGrFMyUoRLExMYjH6CcfyQfsVP \
  --output-mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v \
  --amount 111.94 --decimals 6
```

Реальный `round-trip --usdc-value 10 --profile manual --execute` — отдельный CLI. Ему нужны свой `SOLANA_WALLET_KEY`, USDC/SOL и RPC; без `--execute` он не отправляет сделки. Перед использованием смотрите `--help` и ограничения комиссии.

## Проверки и обновление

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests scripts
.venv/bin/mypy src/backpack_stocks
cargo test --manifest-path rust/pool-math/Cargo.toml
git pull --ff-only
.venv/bin/python -m pip install -e '.[jupiter,dev]'
.venv/bin/python scripts/restart_direct_paper.py --config examples/telegram-direct-paper-1k.toml
```

PostgreSQL-тесты опциональны. `.env`, `state/`, `data/`, `research/`, зависимости и бинарники исключены из Git. В репозитории есть sample `.env` и публичные тестовые фикстуры; собственные секреты и состояние переносятся отдельно.
