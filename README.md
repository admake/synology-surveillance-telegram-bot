# Synology Surveillance Station to Telegram Bot

[![Docker Build and Push](https://github.com/YOUR_USERNAME/synology-surveillance-telegram-bot/actions/workflows/docker-build-push.yml/badge.svg)](https://github.com/YOUR_USERNAME/synology-surveillance-telegram-bot/actions/workflows/docker-build-push.yml)
[![GitHub Container Registry](https://img.shields.io/badge/Container%20Registry-ghcr.io-blue)](https://github.com/YOUR_USERNAME/synology-surveillance-telegram-bot/pkgs/container/synology-surveillance-telegram-bot)

Надежный Docker-контейнер для отправки видео с событий движения из Synology Surveillance Station в Telegram через официальное API.

## Особенности

- ✅ **Надежность**: Активный опрос API вместо реактивных веб-хуков
- ✅ **Без потерь**: Проверка пропущенных событий при перезапуске
- ✅ **Graceful Shutdown**: Корректная обработка остановки контейнера
- ✅ **Health checks**: Встроенная проверка работоспособности
- ✅ **Multi-arch**: Поддержка amd64 и arm64
- ✅ **Безопасность**: Работа от непривилегированного пользователя

## Быстрый старт

### 1. Подготовка Synology

1. В Surveillance Station включите веб-службу API
2. Создайте пользователя API с правами "Просмотр" и "Экспорт"
3. Настройте запись камер по детекции движения (не непрерывную)

### 2. Создание Telegram бота

1. Напишите [@BotFather](https://t.me/botfather) в Telegram
2. Создайте бота и сохраните токен
3. Получите Chat ID: напишите боту, затем перейдите по ссылке: https://api.telegram.org/bot<ВАШ_ТОКЕН>/getUpdates 


### 3. Запуск через Docker Compose

```bash
# Клонируйте репозиторий
git clone https://github.com/YOUR_USERNAME/synology-surveillance-telegram-bot.git
cd synology-surveillance-telegram-bot

# Создайте файл .env
cp .env.example .env
nano .env  # Отредактируйте настройки

# Запустите контейнер
docker-compose -f docker-compose.prod.yml up -d
```
