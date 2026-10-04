# Проверки

## Исходное состояние

Исходный commit `7ed5139670adb3fa7215e835c5be27edec853325`. До изменения кода прошли `make check`, `make race`, `TEST_DATABASE_URL=… make integration` на отдельной PostgreSQL. Рабочее дерево было чистым. Миграции и прежние данные не менялись.

## Автоматизированное покрытие

| № | Сценарий из запроса | Проверка |
|---:|---|---|
| 1 | Пост, корень, ответ | `TestGraphQLFlow`, системный HTTP |
| 2 | Чтение и страницы | общий `TestContract/hierarchy_and_cursor_scope` |
| 3 | Пустая страница, повреждённый курсор | тот же контракт |
| 4 | Курсор другого поста/родителя | контракт + `TestPagesDuringWrites` для batch |
| 5 | Нет поста/родителя | контракт |
| 6 | Родитель другого поста | контракт + реальные PostgreSQL FK |
| 7 | Unicode, 2000/2001, пустой текст | `TestContract/unicode_and_validation` |
| 8 | Запрет чужим пользователем | `TestVerifiedIdentity`, системная проверка JWT и подмены заголовка |
| 9 | Закрыть/открыть | `TestContract/posts_and_authorization`, системный HTTP |
| 10 | Закрытие и множество добавлений | `TestContract/concurrent_close_and_reads` |
| 11 | Параллельные чтения/записи, страницы | тот же контракт, `TestPagesDuringWrites`, held-transaction тесты |
| 12 | Отмена, deadline на lock | контракт rollback/cancellation, `TestClosingHoldsRowLock`, `TestOperationCapacityAndDeadline` |
| 13 | Перезапуск и сохранность | системный restart приложения и PostgreSQL |
| 14 | Новая БД, конкурентный запуск | 3 migrator-процесса одновременно, `TestConcurrentMigrationOnEmptySchema` |
| 15 | Подписка A, запись B/C | `TestSubscriptionsAcrossInstances`, системные реальные HTTP-сокеты |
| 16 | Медленный подписчик, overflow | `TestSlowSubscriberAndPause`; SSE capacity/cancellation отдельно |
| 17 | Потеря/возврат LISTEN | `TestListenerReconnectsAndClosesExistingStreams`, системный terminate backend |
| 18 | Рестарт при активной подписке | системный restart + `bin/watch` с уникальными ID |
| 19 | PostgreSQL временно недоступен | системный stop/start DB, liveness/readiness, сохранность |
| 20 | HTTP/GraphQL/rate/capacity лимиты | `TestDocumentLimits`, `TestBodyRateAndForwardedAddress`, `TestOperationCapacityAndDeadline`, `TestHeartbeatCapacityAndCancellation`, общий Nginx bucket |
| 21 | Нет утечек внутренних данных | `TestStorageErrorsAreSanitized`, JWT HTTP-тест, scan контейнерных логов на реальные ключи/токены/DSN |

Оба хранилища выполняют один service-контракт под race detector. PostgreSQL-тесты работают в отдельных схемах одноразовой БД. Сценарий медленного читателя проверяет детерминированное заполнение hub-буфера без чтения; обычные SSE-сокеты, heartbeat и отмена проверяются отдельно. Это не имитация WAN с packet loss.

Дополнительно проверены 1/2/3 SELECT на страницу размера 1/50/100 и 3 SELECT для 1/10/20 веток. Тесты удержанной незавершённой транзакции запрещают более позднему ID обогнать её commit для комментариев и списка постов. JWT-тесты включают отсутствие, повреждение, неверную подпись, expiry, отсутствие обязательного expiry, подмену X-User-ID, чужого и настоящего автора.

## Команды

- `make verify`: gofmt, `go vet`, unit/HTTP/контракт memory с `-race`, повторная sqlc/gqlgen генерация без изменения файлов.
- `make verify-full`: всё выше, Docker build, PostgreSQL integration с `-race`, три приложения, Nginx, перечисленные системные сценарии; инфраструктура удаляется.
- `make load-smoke`: HTTP + проверка GraphQL errors, 1/3 приложения, шесть сценариев.
- `make load-full`: 3 повтора × 3 конкурентности × 6 сценариев × 2 конфигурации; ресурсные метрики и pg_stat_statements.

Точные артефакты успешного финального запуска и нагрузочных прогонов сохраняются в `docs/performance`. Отсутствующие зависимости, неготовый сервис, ошибка GraphQL, sampler или неожиданный HTTP отказ завершают проверку ненулевым кодом. Ожидаемые rate/capacity отказы в нагрузке учитываются отдельно; при полностью неуспешном сценарии команда также падает.

## Соответствие исходному ТЗ

| Требование | Реализация |
|---|---|
| Go, GraphQL | Go 1.25 toolchain, gqlgen; schema/resolvers |
| Посты, список, чтение | `createPost`, `post`, `posts` |
| Дерево любой глубины | adjacency list, страницы непосредственных детей; контракт проверяет цепочку 200 |
| Автор закрывает обсуждение | проверенный JWT → Service → транзакционная блокировка |
| 2000 Unicode-кодовых точек | общая валидация memory/PG, граничные тесты |
| Пагинация | scope-bound keyset cursor, страницы 1–100, порядок commit |
| Memory/PostgreSQL | флаг `-storage`, единый контракт Store |
| Docker | непривилегированный scratch-образ, Compose, отдельный migrate |
| Unit-тесты | race suite и общие контрактные тесты |
| GraphQL Subscriptions | SSE, межсерверный NOTIFY/LISTEN, heartbeat, recovery feed |

## Фактически выполнено 4 октября 2026

- До правок: `make check`, `make race`, `TEST_DATABASE_URL=… make integration` — прошли на исходном `7ed5139`.
- После правок: `make verify-full` — прошёл локально, включая три одновременно запущенных мигратора, тесты с race, Docker build и системные сценарии; [результат](performance/verification/local-system.json), [интеграционный лог](performance/verification/local-integration.txt).
- GitHub Actions на `bf7f998`: [run 37207166620](https://github.com/maxmashevsky-bit/ozon/actions/runs/37207166620) — `success`; `make verify-full` и чистый `git diff` прошли. [Системный результат](performance/verification/ci-system.json), [интеграционный лог](performance/verification/ci-integration.txt).
- `make load-smoke`, `make load-full`, профиль прямого сравнения — успешно завершились; 108 полных прогонов, 0 GraphQL/HTTP ошибок, 0 пропущенных SSE событий и 0 отключений. [Таблицы, разброс и исходные JSON](performance/README.md).
- Локально выполнена цепочка `up → seed → seed → clean-seed → up-scale → down → up-scale → seed`: seed идемпотентен, очистка затронула только созданные ей записи, хеши всех ранее существовавших постов и комментариев совпали до/после, volume пережил остановку. Результат `.artifacts/local-commands-result.json` остаётся только в локальной игнорируемой директории. Итоговый локальный стенд запущен с тремя приложениями.
- Отдельный бинарник `server -storage=memory` запущен и проверен настоящим HTTP: JWT, пост, корень, ответ, recovery feed и штатное завершение по SIGTERM — успешно. `scripts/manage.py init` также выполнен без обращения к Docker.

Тесты миграций на пустой БД первоначально выявили гонку создания `goose_db_version`, которая не покрывалась только внутренней блокировкой Goose. Перед финальным запуском добавлена внешняя session lock на весь вызов мигратора. Первый пробный замер SSE был исключён из отчёта из-за уведомлений от заполнения fixture при активном LISTEN; исправленный сценарий останавливает приложения на время подготовки данных. Оба факта и исправления сохранены в истории работы, а результаты отчёта относятся к проверенному коду.
