# Telegram Page Sender

Расширение добавляет кнопку справа от `FUNDOOR SOCKET` на странице Fundoor Spread.
По нажатию оно
отправляет текущий URL, заголовок страницы и введённый текст в защищённый relay
бота. Telegram `BOT_TOKEN` в расширении не хранится.

## 1. Настройка бота

Добавьте на сервере в `.env`:

```env
BROWSER_RELAY_ENABLED=1
BROWSER_RELAY_HOST=127.0.0.1
BROWSER_RELAY_PORT=8765
BROWSER_RELAY_PATH=/browser-share
BROWSER_RELAY_SECRET=replace-with-a-random-secret-at-least-24-characters
```

Сгенерировать секрет можно командой:

```bash
openssl rand -hex 32
```

После перезапуска бота настройте получателя:

```text
/list -> БОТ -> КНОПКА В БРАУЗЕРЕ -> ИЗМЕНИТЬ ЧАТ / ТОПИК
```

Можно указать только ID чата или ID чата и ID топика через пробел.

## 2. Публичный HTTPS endpoint

Не публикуйте встроенный HTTP server напрямую. Пример location для nginx:

```nginx
location = /browser-share {
    proxy_pass http://127.0.0.1:8765/browser-share;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    client_max_body_size 16k;
}
```

Используйте существующий HTTPS-домен и сертификат. После изменения конфигурации
проверьте nginx и перезагрузите его.

## 3. Установка расширения

1. Откройте `chrome://extensions` или `edge://extensions`.
2. Включите режим разработчика.
3. Нажмите `Загрузить распакованное расширение`.
4. Выберите папку `browser_share_extension`.
5. Оставьте адрес страницы `https://fundoor.pro/spreadchart`, укажите публичный
   HTTPS endpoint, тот же secret и текст.
6. Сохраните настройки и обновите целевую страницу.

Кнопка появится на странице с настроенным доменом и путём, в том числе если
меняются параметры `base`, `long`, `short`, `interval` и другие. Перед отправкой
текст можно изменить. В Telegram уйдёт полный адрес текущей пары.
