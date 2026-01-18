#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Исправленная версия с корректной обработкой времени и длительности видео
"""

import os
import json
import time
import signal
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from dataclasses import dataclass
import tempfile

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


@dataclass
class Recording:
    """Класс для представления записи с камеры"""

    id: str
    camera_id: str
    start_time: int  # Unix timestamp в секундах
    duration: int  # Длительность в секундах
    size: int  # Размер в байтах
    file_path: Optional[str] = None


class SynologyAPI:
    """Клиент для работы с API Synology Surveillance Station"""

    def __init__(self):
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"

        self.session = requests.Session()
        self.session.verify = os.getenv("SSL_VERIFY", "false").lower() == "true"
        self.sid = None
        self.last_login = None
        self.cameras_cache: Dict[str, Dict] = {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(RequestException),
    )
    def login(self) -> bool:
        """Аутентификация в API Synology"""
        try:
            params = {
                "api": "SYNO.API.Auth",
                "version": "7",
                "method": "login",
                "account": os.getenv("SYNO_USER"),
                "passwd": os.getenv("SYNO_PASS"),
                "session": "SurveillanceStation",
                "format": "cookie",
            }

            if os.getenv("SYNO_OTP"):
                params["otp_code"] = os.getenv("SYNO_OTP")

            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                self.sid = data["data"]["sid"]
                self.last_login = time.time()
                logger.info("✅ Аутентификация успешна")
                return True

            logger.error(f"❌ Аутентификация не удалась: {data}")
            return False

        except RequestException as e:
            logger.error(f"❌ Ошибка сети при аутентификации: {e}")
            raise

    def ensure_session(self) -> bool:
        """Убеждаемся, что сессия активна"""
        if not self.sid or not self.last_login or (time.time() - self.last_login > 600):
            return self.login()
        return True

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def get_cameras(self) -> Dict[str, Dict]:
        """Получаем список всех камер и кэшируем"""
        if not self.ensure_session():
            return {}

        try:
            params = {
                "api": "SYNO.SurveillanceStation.Camera",
                "method": "List",
                "version": "9",
                "_sid": self.sid,
            }

            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                cameras = data.get("data", {}).get("cameras", [])

                # Кэшируем информацию о камерах
                self.cameras_cache = {
                    str(cam["id"]): {
                        "id": cam["id"],
                        "name": cam.get(
                            "newName", cam.get("name", f'Камера {cam["id"]}')
                        ),
                        "ip": cam.get("ip", "N/A"),
                        "model": cam.get("model", "N/A"),
                    }
                    for cam in cameras
                }

                logger.info(f"📹 Загружено {len(cameras)} камер")
                return self.cameras_cache

            logger.warning(f"⚠️ Не удалось получить список камер: {data}")
            return {}

        except RequestException as e:
            logger.error(f"❌ Ошибка при получении камер: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def get_recordings(
        self, camera_id: Optional[str] = None, limit: int = 10
    ) -> List[Recording]:
        """Получаем список записей"""
        if not self.ensure_session():
            return []

        try:
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "List",
                "version": "9",  # Используем версию 9 для получения полной информации
                "_sid": self.sid,
                "offset": "0",
                "limit": str(limit),
            }

            if camera_id:
                params["cameraIds"] = str(camera_id)

            response = self.session.get(self.base_url, params=params, timeout=20)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                recordings_data = data.get("data", {}).get("recordings", [])

                recordings = []
                for rec in recordings_data:
                    try:
                        # Преобразуем данные в объект Recording
                        recording = Recording(
                            id=str(rec.get("id")),
                            camera_id=str(rec.get("cameraId", "unknown")),
                            start_time=rec.get("startTime", 0),
                            duration=rec.get("duration", 0)
                            // 1000,  # Конвертируем мс в секунды
                            size=rec.get("size", 0),
                        )
                        recordings.append(recording)
                    except Exception as e:
                        logger.warning(
                            f"⚠️ Ошибка обработки записи {rec.get('id')}: {e}"
                        )
                        continue

                logger.info(f"🎥 Получено {len(recordings)} записей")
                return recordings

            logger.warning(f"⚠️ Нет записей или ошибка API: {data}")
            return []

        except RequestException as e:
            logger.error(f"❌ Ошибка при получении записей: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def download_full_recording(self, recording: Recording) -> Optional[str]:
        """Скачивает полную запись целиком"""
        if not self.ensure_session():
            return None

        try:
            # Создаем временный файл
            temp_file = tempfile.NamedTemporaryFile(
                suffix=".mp4", delete=False, dir="/tmp"
            )
            temp_file.close()

            # Скачиваем всю запись (без параметров offsetTimeMs и playTimeMs)
            download_url = f"{self.base_url}/temp.mp4"
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": "1",  # Самая простая версия для скачивания целиком
                "_sid": self.sid,
                "id": recording.id,
            }

            logger.info(
                f"📥 Начинаю скачивание записи {recording.id} "
                f"(длительность: {recording.duration} сек, "
                f"размер: {recording.size / (1024*1024):.1f} МБ)"
            )

            response = self.session.get(
                download_url,
                params=params,
                stream=True,
                timeout=120,  # Увеличиваем таймаут для больших файлов
            )
            response.raise_for_status()

            # Скачиваем с прогрессом
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(temp_file.name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Логируем прогресс каждые 5 МБ
                        if total_size > 0 and downloaded % (5 * 1024 * 1024) < 8192:
                            percent = (downloaded / total_size) * 100
                            logger.debug(
                                f"📥 Прогресс скачивания: {percent:.1f}% "
                                f"({downloaded/(1024*1024):.1f} МБ / {total_size/(1024*1024):.1f} МБ)"
                            )

            # Проверяем размер скачанного файла
            file_size = os.path.getsize(temp_file.name)
            logger.info(
                f"✅ Запись {recording.id} скачана, размер: {file_size/(1024*1024):.1f} МБ"
            )

            return temp_file.name

        except RequestException as e:
            logger.error(f"❌ Ошибка скачивания записи {recording.id}: {e}")

            # Удаляем частично скачанный файл
            try:
                if os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
            except:
                pass

            raise
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при скачивании: {e}")
            raise

    def get_camera_name(self, camera_id: str) -> str:
        """Получает имя камеры по ID"""
        if not self.cameras_cache:
            self.get_cameras()

        camera = self.cameras_cache.get(str(camera_id))
        return (
            camera.get("name", f"Камера {camera_id}")
            if camera
            else f"Камера {camera_id}"
        )


class TelegramBot:
    """Клиент для отправки сообщений в Telegram"""

    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 МБ - лимит Telegram для видео

    def __init__(self):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

        # Проверяем доступность бота
        self.test_connection()

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def test_connection(self):
        """Проверяем соединение с Telegram API"""
        try:
            response = requests.get(f"{self.base_url}/getMe", timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get("ok"):
                bot_name = data["result"]["first_name"]
                logger.info(f"🤖 Бот {bot_name} подключен к Telegram")
            else:
                logger.error(f"❌ Ошибка Telegram API: {data}")

        except Exception as e:
            logger.error(f"❌ Не удалось подключиться к Telegram: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def send_video(self, video_path: str, caption: str = "") -> bool:
        """Отправляет видео в Telegram"""
        try:
            # Проверяем размер файла
            file_size = os.path.getsize(video_path)

            if file_size > self.MAX_FILE_SIZE:
                logger.warning(
                    f"⚠️ Файл слишком большой ({file_size/(1024*1024):.1f} МБ > "
                    f"{self.MAX_FILE_SIZE/(1024*1024):.1f} МБ). Telegram не примет."
                )
                return False

            logger.info(
                f"📤 Отправляю видео в Telegram ({file_size/(1024*1024):.1f} МБ)"
            )

            with open(video_path, "rb") as video_file:
                files = {"video": video_file}
                data = {
                    "chat_id": self.chat_id,
                    "caption": caption,
                    "supports_streaming": True,
                    "parse_mode": "HTML",
                }

                response = requests.post(
                    f"{self.base_url}/sendVideo",
                    files=files,
                    data=data,
                    timeout=60,  # Увеличиваем таймаут для больших файлов
                )
                response.raise_for_status()

            logger.info("✅ Видео отправлено в Telegram")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка отправки видео: {e}")
            raise


class StateManager:
    """Управление состоянием обработанных записей"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.processed_ids = set()
        self.last_processed_time = 0
        self.load_state()

    def load_state(self) -> None:
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    self.processed_ids = set(state.get("processed_ids", []))
                    self.last_processed_time = state.get("last_processed_time", 0)

                    logger.info(
                        f"📂 Загружено состояние: {len(self.processed_ids)} обработанных записей"
                    )

                    # Очищаем старые записи (старше 7 дней)
                    self.cleanup_old_records()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")
            # При первой загрузке смотрим записи за последний час
            self.last_processed_time = int(time.time() - 3600)

    def save_state(self) -> None:
        """Сохраняет состояние в файл"""
        try:
            state = {
                "processed_ids": list(self.processed_ids),
                "last_processed_time": self.last_processed_time,
                "updated_at": datetime.now().isoformat(),
            }

            # Создаем директорию если не существует
            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            logger.debug("💾 Состояние сохранено")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")

    def is_processed(self, recording_id: str) -> bool:
        """Проверяет, была ли запись обработана"""
        return recording_id in self.processed_ids

    def mark_processed(self, recording_id: str) -> None:
        """Помечает запись как обработанную"""
        self.processed_ids.add(recording_id)
        self.last_processed_time = int(time.time())

    def cleanup_old_records(self, max_age_days: int = 7) -> None:
        """Очищает старые записи из состояния"""
        if len(self.processed_ids) > 1000:
            # Ограничиваем количество хранимых ID
            self.processed_ids = set(list(self.processed_ids)[-1000:])
            logger.debug(
                f"🧹 Очищены старые записи, осталось: {len(self.processed_ids)}"
            )


def format_duration(seconds: int) -> str:
    """Форматирует длительность в человекочитаемый вид"""
    if seconds < 60:
        return f"{seconds} сек"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return (
            f"{minutes} мин {remaining_seconds} сек"
            if remaining_seconds > 0
            else f"{minutes} мин"
        )
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours} ч {minutes} мин" if minutes > 0 else f"{hours} ч"


def format_caption(recording: Recording, camera_name: str) -> str:
    """Форматирует подпись для Telegram"""
    try:
        # Преобразуем timestamp в читаемое время
        start_time = datetime.fromtimestamp(recording.start_time)

        # Форматируем дату и время
        time_str = start_time.strftime("%d.%m.%Y %H:%M:%S")

        # Форматируем длительность
        duration_str = format_duration(recording.duration)

        # Форматируем размер
        size_mb = recording.size / (1024 * 1024)
        size_str = f"{size_mb:.1f} МБ" if size_mb > 0 else "размер неизвестен"

        # Создаем подпись
        caption = (
            f"<b>🎥 Движение обнаружено</b>\n\n"
            f"<b>📷 Камера:</b> {camera_name}\n"
            f"<b>🕐 Время:</b> {time_str}\n"
            f"<b>⏱️ Длительность:</b> {duration_str}\n"
            f"<b>📦 Размер:</b> {size_str}\n\n"
            f"<i>#surveillance</i>"
        )

        return caption

    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return "🎥 Обнаружено движение"


def main():
    """Основная функция приложения"""
    logger.info("🚀 Запуск Surveillance Station Telegram Bot")

    # Проверка обязательных переменных
    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Отсутствуют обязательные переменные: {missing_vars}")
        return

    # Инициализация компонентов
    synology = SynologyAPI()
    telegram = TelegramBot()
    state = StateManager(os.getenv("STATE_FILE", "/data/state.json"))

    # Получаем список камер
    cameras = synology.get_cameras()
    camera_id = os.getenv("CAMERA_ID", "5")
    camera_name = synology.get_camera_name(camera_id)

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")

    # Настройка graceful shutdown
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"🛑 Получен сигнал {signum}, завершаю работу...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Основные параметры
    check_interval = int(os.getenv("CHECK_INTERVAL", "30"))

    # Основной цикл
    logger.info("🔄 Начинаю мониторинг записей...")

    while not shutdown_requested:
        try:
            # Получаем новые записи
            recordings = synology.get_recordings(camera_id=camera_id, limit=20)

            # Обрабатываем записи в порядке от старых к новым
            new_recordings = 0

            for recording in recordings:
                # Пропускаем уже обработанные
                if state.is_processed(recording.id):
                    continue

                # Пропускаем записи без длительности
                if recording.duration <= 0:
                    logger.warning(
                        f"⚠️ Пропускаю запись {recording.id} без длительности"
                    )
                    continue

                logger.info(
                    f"🆕 Новая запись: ID={recording.id}, "
                    f"длительность={format_duration(recording.duration)}"
                )

                try:
                    # Скачиваем полную запись
                    video_path = synology.download_full_recording(recording)

                    if video_path:
                        # Формируем подпись
                        caption = format_caption(recording, camera_name)

                        # Отправляем в Telegram
                        if telegram.send_video(video_path, caption):
                            state.mark_processed(recording.id)
                            new_recordings += 1
                            logger.info(f"✅ Запись {recording.id} успешно обработана")

                        # Удаляем временный файл
                        try:
                            os.remove(video_path)
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")

                except Exception as e:
                    logger.error(f"❌ Ошибка обработки записи {recording.id}: {e}")

                # Проверяем флаг shutdown
                if shutdown_requested:
                    break

            if new_recordings > 0:
                logger.info(f"📊 Обработано новых записей: {new_recordings}")

            # Сохраняем состояние
            state.save_state()

            # Ждем следующей проверки
            for i in range(check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("🛑 Прерывание с клавиатуры")
            shutdown_requested = True
            break
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка в основном цикле: {e}")
            time.sleep(10)  # Пауза при серьезной ошибке

    # Завершение работы
    logger.info("👋 Завершение работы бота")
    state.save_state()


if __name__ == "__main__":
    main()
