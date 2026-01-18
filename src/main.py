#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Исправленная версия с правильными API endpoints из старого кода
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
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")

        # Базовый URL как в старом коде
        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"

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
        """Аутентификация в API Synology (как в старом коде)"""
        try:
            params = {
                "api": "SYNO.API.Auth",
                "version": "7",
                "method": "login",
                "account": os.getenv("SYNO_USER"),
                "passwd": os.getenv("SYNO_PASS"),
                "session": "SurveillanceStation",
                "format": "cookie",  # Из старого кода
            }

            # Добавляем OTP если есть (из старого кода)
            if os.getenv("SYNO_OTP"):
                params["otp_code"] = os.getenv("SYNO_OTP")

            response = self.session.get(self.base_url, params=params, timeout=10)
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

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def get_cameras(self):
        """Получает список всех камер (как в старом коде)"""
        if not self.is_session_valid():
            self.login()

        try:
            params = {
                "api": "SYNO.SurveillanceStation.Camera",
                "method": "List",
                "version": "9",  # Из старого кода
                "_sid": self.sid,
            }

            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                cameras = data.get("data", {}).get("cameras", [])
                logger.info(f"Retrieved {len(cameras)} cameras")
                return cameras

            logger.warning(f"No cameras or API error: {data}")
            return []

        except RequestException as e:
            logger.error(f"Error fetching cameras: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def get_recordings(self, camera_id=None, limit=10, offset=0):
        """Получает список записей (как в старом коде - Recording API вместо Event API)"""
        if not self.is_session_valid():
            self.login()

        try:
            params = {
                "api": "SYNO.SurveillanceStation.Recording",  # Recording API, не Event!
                "method": "List",
                "version": "6",  # Из старого кода
                "_sid": self.sid,
                "offset": str(offset),
                "limit": str(limit),
                "fromTime": "0",  # Из старого кода
                "toTime": "0",  # Из старого кода
            }

            if camera_id:
                params["cameraIds"] = str(camera_id)

            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                recordings = data.get("data", {}).get("recordings", [])
                logger.info(f"Retrieved {len(recordings)} recordings")
                return recordings

            logger.warning(f"No recordings or API error: {data}")
            return []

        except RequestException as e:
            logger.error(f"Error fetching recordings: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def download_recording(self, recording_id, offset_ms=0, duration_ms=10000):
        """Скачивает видео записи (как в старом коде)"""
        if not self.is_session_valid():
            self.login()

        try:
            # URL для скачивания как в старом коде: base_url + '/temp.mp4'
            download_url = f"{self.base_url}/temp.mp4"

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": "6",  # Из старого кода
                "_sid": self.sid,
                "id": recording_id,
                "mountId": "0",  # Из старого кода
                "offsetTimeMs": str(offset_ms),  # Из старого кода
                "playTimeMs": str(duration_ms),  # Из старого кода
            }

            response = self.session.get(
                download_url, params=params, stream=True, timeout=30
            )
            response.raise_for_status()

            return response.content  # Возвращаем бинарное содержимое

        except RequestException as e:
            logger.error(f"Error downloading recording {recording_id}: {e}")
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
    def send_video(self, video_content, caption=""):
        """Отправляет видео в Telegram"""
        try:
            # Создаем временный файл
            temp_file = f"/tmp/video_{int(time.time())}.mp4"

            with open(temp_file, "wb") as f:
                f.write(video_content)

            # Отправляем файл
            with open(temp_file, "rb") as video_file:
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

            # Удаляем временный файл
            os.remove(temp_file)

            logger.info("Video sent to Telegram")
            return True

        except Exception as e:
            logger.error(f"Error sending video to Telegram: {e}")
            # Пытаемся удалить временный файл в случае ошибки
            try:
                os.remove(temp_file)
            except:
                pass
            raise


class StateManager:
    """Управление состоянием обработанных записей"""

    def __init__(self, state_file):
        self.state_file = Path(state_file)
        self.processed_recordings = set()
        self.last_check_time = None
        self.last_recording_id = None
        self.load_state()

    def load_state(self):
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    self.processed_recordings = set(
                        state.get("processed_recordings", [])
                    )
                    self.last_check_time = state.get("last_check_time")
                    self.last_recording_id = state.get("last_recording_id")
                    logger.info(
                        f"Loaded state with {len(self.processed_recordings)} processed recordings"
                    )

                    # Если нет времени последней проверки, ставим час назад
                    if self.last_check_time is None:
                        self.last_check_time = int(time.time() - 3600)

        except Exception as e:
            logger.warning(f"Could not load state: {e}")
            self.last_check_time = int(time.time() - 3600)  # 1 час назад

    def save_state(self):
        """Сохраняет состояние в файл"""
        try:
            state = {
                "processed_recordings": list(self.processed_recordings),
                "last_check_time": self.last_check_time,
                "last_recording_id": self.last_recording_id,
                "updated_at": datetime.now().isoformat(),
            }

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            logger.debug("State saved successfully")
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def is_recording_processed(self, recording_id):
        """Проверяет, была ли запись уже обработана"""
        return recording_id in self.processed_recordings

    def mark_recording_processed(self, recording_id):
        """Помечает запись как обработанную"""
        self.processed_recordings.add(recording_id)
        self.last_recording_id = recording_id

    def cleanup_old_recordings(self, max_age_days=7):
        """Очищает старые записи из состояния"""
        if len(self.processed_recordings) > 1000:
            self.processed_recordings = set(list(self.processed_recordings)[-1000:])


def get_camera_name(cameras, camera_id):
    """Получает имя камеры по ID"""
    for camera in cameras:
        if str(camera.get("id")) == str(camera_id):
            return camera.get("newName", camera.get("name", f"Camera {camera_id}"))
    return f"Camera {camera_id}"


def main():
    """Основная функция приложения"""
    logger.info("Starting Surveillance Station to Telegram Bot (Fixed API version)")

    # Проверка обязательных переменных
    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            return

    # Инициализация компонентов
    synology = SynologyAPI()
    telegram = TelegramBot()
    state = StateManager(os.getenv("STATE_FILE", "/data/state.json"))

    # Получаем список камер для имен
    cameras = []
    try:
        cameras = synology.get_cameras()
        logger.info(f"Found {len(cameras)} cameras")
    except Exception as e:
        logger.error(f"Could not get cameras list: {e}")

    camera_id = os.getenv("CAMERA_ID")
    if camera_id and cameras:
        camera_name = get_camera_name(cameras, camera_id)
        logger.info(f"Monitoring camera: {camera_name} (ID: {camera_id})")

    # Graceful shutdown флаг
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Received signal {signum}, initiating shutdown")
        shutdown_requested = True

    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    check_interval = int(os.getenv("CHECK_INTERVAL", "30"))

    logger.info("Bot started successfully")

    # Основной цикл
    while not shutdown_requested:
        try:
            # Получаем последние записи
            recordings = synology.get_recordings(camera_id=camera_id, limit=10)

            # Обрабатываем записи в обратном порядке (сначала новые)
            for recording in reversed(recordings):
                recording_id = recording.get("id")

                # Пропускаем уже обработанные записи
                if state.is_recording_processed(recording_id):
                    continue

                logger.info(f"New recording detected: {recording_id}")

                try:
                    # Скачиваем первые 10 секунд записи (как в старом коде)
                    video_content = synology.download_recording(
                        recording_id, offset_ms=0, duration_ms=10000
                    )

                    # Формируем подпись
                    rec_time = datetime.fromtimestamp(
                        recording.get("startTime", time.time())
                    )
                    rec_camera_id = recording.get("cameraId", camera_id)
                    camera_name = get_camera_name(cameras, rec_camera_id)

                    caption = (
                        f"📹 Обнаружено движение\n"
                        f"📷 Камера: {camera_name}\n"
                        f"🕐 Время: {rec_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"⏱️ Длительность: {recording.get('duration', 0)} сек"
                    )

                    # Отправляем в Telegram
                    if telegram.send_video(video_content, caption):
                        # Помечаем как обработанную
                        state.mark_recording_processed(recording_id)
                        logger.info(f"Successfully processed recording {recording_id}")
                    else:
                        logger.error(f"Failed to send recording {recording_id}")

                except Exception as e:
                    logger.error(f"Error processing recording {recording_id}: {e}")

                # Проверяем флаг shutdown после каждой записи
                if shutdown_requested:
                    logger.info("Shutdown requested, breaking recording loop")
                    break

            # Обновляем время последней проверки и сохраняем состояние
            state.last_check_time = int(time.time())
            state.save_state()

            # Очистка старых событий раз в час
            if int(time.time()) % 3600 < check_interval:
                state.cleanup_old_recordings()

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
