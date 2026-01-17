#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Надежная отправка видео с событий движения в Telegram
"""

import os
import json
import time
import signal
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from requests.exceptions import RequestException

# Настройка структурированного логирования
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


class SynologyAPI:
    """Клиент для работы с API Synology Surveillance Station"""

    def __init__(self):
        self.base_url = (
            f"https://{os.getenv('SYNO_IP')}:{os.getenv('SYNO_PORT', '5001')}/webapi"
        )
        self.session = requests.Session()
        self.session.verify = os.getenv("SSL_VERIFY", "false").lower() == "true"
        self.sid = None
        self.last_login = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(RequestException),
    )
    def login(self):
        """Аутентификация в API Synology"""
        try:
            auth_url = f"{self.base_url}/auth.cgi"

            # Первый шаг: получение sid
            params = {
                "api": "SYNO.API.Auth",
                "method": "login",
                "version": "7",
                "account": os.getenv("SYNO_USER"),
                "passwd": os.getenv("SYNO_PASS"),
                "session": "SurveillanceStation",
                "format": "sid",
            }

            response = self.session.get(auth_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                self.sid = data["data"]["sid"]
                self.last_login = time.time()
                logger.info("Successfully authenticated to Synology API")
                return True

            logger.error(f"Authentication failed: {data}")
            return False

        except RequestException as e:
            logger.error(f"Network error during authentication: {e}")
            raise

    def is_session_valid(self):
        """Проверяет валидность текущей сессии"""
        if not self.sid or not self.last_login:
            return False
        # Сессия истекает через 10 минут
        return (time.time() - self.last_login) < 600

    # В файле src/surveillance_bot.py найдите функцию get_events и ЗАМЕНИТЕ её:

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def get_events(self, start_time, end_time, camera_id=None):
        """Получает список событий движения - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        if not self.is_session_valid():
            self.login()

        try:
            # Используем правильный endpoint и версию API
            event_url = f"{self.base_url}/webapi/entry.cgi"

            # БАЗОВЫЕ параметры для версии 9 (которая работает)
            params = {
                "api": "SYNO.SurveillanceStation.Camera.Event",
                "method": "list",
                "version": "9",  # ← ВАЖНО: версия 9 из диагностики
                "_sid": self.sid,
                "fromTime": start_time,
                "toTime": end_time,
            }

            # Добавляем cameraIds только если указан
            if camera_id:
                params["cameraIds"] = str(camera_id)

            # Пробуем разные комбинации параметров если первая не сработает
            test_cases = [
                params,  # 1. Без фильтров
                {**params, "eventFilter": "motion"},  # 2. С фильтром движения
                {**params, "blIncludeSnapshot": "false"},  # 3. Без снимков
                {**params, "limit": "100", "offset": "0"},  # 4. С лимитом
            ]

            for i, test_params in enumerate(test_cases):
                try:
                    logger.debug(f"Пробуем параметры #{i+1}: {test_params}")
                    response = self.session.get(
                        event_url, params=test_params, timeout=15
                    )
                    response.raise_for_status()

                    data = response.json()
                    if data.get("success"):
                        events = data.get("data", {}).get("events", [])
                        logger.info(
                            f"Retrieved {len(events)} events with params #{i+1}"
                        )
                        return events
                    else:
                        logger.debug(f"Params #{i+1} failed: {data.get('error')}")

                except Exception as e:
                    logger.debug(f"Params #{i+1} error: {e}")
                    continue

            # Если ни один вариант не сработал
            logger.warning("All parameter combinations failed for events API")
            return []

        except RequestException as e:
            logger.error(f"Error fetching events: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def download_event(self, event_id, output_path):
        """Скачивает видео события"""
        if not self.is_session_valid():
            self.login()

        try:
            download_url = f"{self.base_url}/SurveillanceStation/camera.cgi"
            params = {
                "api": "SYNO.SurveillanceStation.Camera.Event",
                "method": "download",
                "version": "1",
                "_sid": self.sid,
                "id": event_id,
                "downloadType": "file",
            }

            response = self.session.get(
                download_url, params=params, stream=True, timeout=30
            )
            response.raise_for_status()

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded event {event_id} to {output_path}")
            return True

        except RequestException as e:
            logger.error(f"Error downloading event {event_id}: {e}")
            raise


class TelegramBot:
    """Клиент для отправки сообщений в Telegram"""

    def __init__(self):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def send_video(self, video_path, caption=""):
        """Отправляет видео в Telegram"""
        try:
            with open(video_path, "rb") as video_file:
                files = {"video": video_file}
                data = {
                    "chat_id": self.chat_id,
                    "caption": caption,
                    "supports_streaming": True,
                }

                response = requests.post(
                    f"{self.base_url}/sendVideo", files=files, data=data, timeout=60
                )
                response.raise_for_status()

            logger.info(f"Video sent to Telegram: {video_path}")
            return True

        except RequestException as e:
            logger.error(f"Error sending video to Telegram: {e}")
            raise


class StateManager:
    """Управление состоянием обработанных событий"""

    def __init__(self, state_file):
        self.state_file = Path(state_file)
        self.processed_events = set()
        self.last_check_time = None
        self.load_state()

    def load_state(self):
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    self.processed_events = set(state.get("processed_events", []))
                    self.last_check_time = state.get("last_check_time")
                    logger.info(
                        f"Loaded state with {len(self.processed_events)} processed events"
                    )
        except Exception as e:
            logger.warning(f"Could not load state: {e}")
            # При первой загрузке проверяем последние N минут
            self.last_check_time = int(
                time.time() - int(os.getenv("LOOKBACK_MINUTES", 5)) * 60
            )

    def save_state(self):
        """Сохраняет состояние в файл"""
        try:
            state = {
                "processed_events": list(self.processed_events),
                "last_check_time": self.last_check_time,
                "updated_at": datetime.now().isoformat(),
            }

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            logger.debug("State saved successfully")
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def is_event_processed(self, event_id):
        """Проверяет, было ли событие уже обработано"""
        return event_id in self.processed_events

    def mark_event_processed(self, event_id):
        """Помечает событие как обработанное"""
        self.processed_events.add(event_id)

    def cleanup_old_events(self, max_age_days=7):
        """Очищает старые события из состояния"""
        # В этой реализации просто ограничиваем размер множества
        if len(self.processed_events) > 1000:
            # Оставляем только последние 1000 событий
            self.processed_events = set(list(self.processed_events)[-1000:])


def main():
    """Основная функция приложения"""
    logger.info("Starting Surveillance Station to Telegram Bot")

    # Инициализация компонентов
    synology = SynologyAPI()
    telegram = TelegramBot()
    state = StateManager(os.getenv("STATE_FILE", "/data/state.json"))

    # Graceful shutdown флаг
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Received signal {signum}, initiating shutdown")
        shutdown_requested = True

    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    check_interval = int(os.getenv("CHECK_INTERVAL", 30))
    camera_id = os.getenv("CAMERA_ID")

    # Основной цикл
    while not shutdown_requested:
        try:
            # Определяем временной диапазон для проверки
            end_time = int(time.time())
            start_time = state.last_check_time or (
                end_time - int(os.getenv("LOOKBACK_MINUTES", 5)) * 60
            )

            logger.debug(
                f"Checking events from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}"
            )

            # Получаем события движения
            events = synology.get_events(start_time, end_time, camera_id)

            # Обрабатываем каждое событие
            for event in events:
                event_id = event.get("id")

                # Пропускаем уже обработанные события
                if state.is_event_processed(event_id):
                    logger.debug(f"Event {event_id} already processed, skipping")
                    continue

                # Создаем временный файл для видео
                temp_file = f"/tmp/event_{event_id}_{int(time.time())}.mp4"

                try:
                    # Скачиваем видео события
                    if synology.download_event(event_id, temp_file):
                        # Формируем подпись
                        event_time = datetime.fromtimestamp(
                            event.get("startTime", time.time())
                        )
                        caption = f"🚨 Движение обнаружено\n📷 Камера: {event.get('cameraName', 'Unknown')}\n🕐 Время: {event_time.strftime('%Y-%m-%d %H:%M:%S')}"

                        # Отправляем в Telegram
                        if telegram.send_video(temp_file, caption):
                            # Помечаем как обработанное
                            state.mark_event_processed(event_id)
                            logger.info(f"Successfully processed event {event_id}")

                except Exception as e:
                    logger.error(f"Error processing event {event_id}: {e}")

                finally:
                    # Удаляем временный файл
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception as e:
                        logger.warning(f"Could not delete temp file {temp_file}: {e}")

                # Проверяем флаг shutdown после каждого события
                if shutdown_requested:
                    logger.info("Shutdown requested, breaking event loop")
                    break

            # Обновляем время последней проверки
            state.last_check_time = end_time
            state.save_state()

            # Очистка старых событий раз в час
            if int(time.time()) % 3600 < check_interval:
                state.cleanup_old_events()

            # Ждем следующей проверки
            for _ in range(check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            shutdown_requested = True
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(10)  # Пауза при ошибке

    # Завершение работы
    logger.info("Application shutdown complete")
    state.save_state()


if __name__ == "__main__":
    main()
