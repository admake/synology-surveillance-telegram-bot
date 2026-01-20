#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Модульная и поддерживаемая версия
"""

import os
import json
import time
import signal
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple, Any
from dataclasses import dataclass, field
import tempfile

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from requests.exceptions import RequestException

# ============================================================================
# Настройка логирования
# ============================================================================

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Конфигурация
# ============================================================================


@dataclass
class AppConfig:
    """Конфигурация приложения"""

    check_interval: int = 10
    fragment_duration_ms: int = 10000
    log_level: str = "INFO"
    state_file: str = "/data/state.json"
    camera_id: str = "5"
    video_estimated_duration_ms: int = 30000
    video_max_fragments: int = 3
    cleanup_max_age_hours: int = 24

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Создает конфигурацию из переменных окружения"""
        config = cls()

        if os.getenv("CHECK_INTERVAL"):
            config.check_interval = int(os.getenv("CHECK_INTERVAL"))
        if os.getenv("FRAGMENT_DURATION_MS"):
            config.fragment_duration_ms = int(os.getenv("FRAGMENT_DURATION_MS"))

        config.log_level = os.getenv("LOG_LEVEL", config.log_level).upper()
        config.state_file = os.getenv("STATE_FILE", config.state_file)
        config.camera_id = os.getenv("CAMERA_ID", config.camera_id)

        return config


# ============================================================================
# Модели данных
# ============================================================================


@dataclass
class Recording:
    """Класс для представления записи с камеры"""

    id: str
    camera_id: str
    start_time: int
    duration: int
    size: int


@dataclass
class FragmentProgress:
    """Прогресс отправки фрагментов записи"""

    recording_id: str
    next_offset_ms: int = 0
    fragments_sent: int = 0
    last_attempt_time: float = 0
    consecutive_fails: int = 0
    is_completed: bool = False
    estimated_duration_ms: int = 0
    last_seen_time: float = 0
    full_duration_checked: bool = False


# ============================================================================
# Утилиты
# ============================================================================


def get_video_duration(file_path: str) -> Tuple[float, bool]:
    """
    Получает реальную длительность видеофайла в секундах через ffprobe
    Возвращает (длительность, успех_определения)
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ Файл не найден: {file_path}")
            return 0.0, False

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.warning(f"⚠️ Файл пустой: {file_path}")
            return 0.0, False

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            file_path,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)

            if result.returncode == 0:
                duration_str = result.stdout.strip()
                if duration_str:
                    duration = float(duration_str)
                    logger.debug(
                        f"📊 FFprobe: видео {file_path}, длительность={duration:.2f} сек"
                    )
                    return duration, True
                else:
                    logger.warning(
                        f"⚠️ FFprobe вернул пустой результат для {file_path}"
                    )
            else:
                logger.warning(f"⚠️ FFprobe вернул ошибку: {result.stderr}")

        except subprocess.TimeoutExpired:
            logger.warning(
                f"⚠️ Таймаут при определении длительности видео: {file_path}"
            )
        except FileNotFoundError:
            logger.warning(f"⚠️ FFprobe не найден. Установите ffmpeg в контейнер.")
        except ValueError:
            logger.warning(
                f"⚠️ Не могу преобразовать результат ffprobe в число: {result.stdout}"
            )

        # Альтернативный метод
        try:
            with open(file_path, "rb") as f:
                f.seek(0)
                data = f.read(10000)

                if b"moov" in data or b"ftyp" in data:
                    logger.debug(
                        f"📊 Альтернативный метод: видео {file_path}, определяем как MP4"
                    )
                    approx_duration = file_size / (100 * 1024)
                    return approx_duration, True

        except Exception as e:
            logger.debug(f"⚠️ Ошибка альтернативного определения длительности: {e}")

        logger.warning(f"⚠️ Не удалось определить длительность видео: {file_path}")
        return 0.0, False

    except Exception as e:
        logger.error(f"❌ Ошибка при определении длительности видео: {e}")
        return 0.0, False


# ============================================================================
# Synology API
# ============================================================================


class SynologyAPI:
    """Клиент для работы с API Synology Surveillance Station"""

    def __init__(self):
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"

        self.session = requests.Session()
        self.session.verify = True
        self.sid = None
        self.last_login = None
        self.cameras_cache: Dict[str, Dict] = {}
        self.api_version = "6"

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
        self,
        camera_id: Optional[str] = None,
        limit: int = 20,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
    ) -> List[Recording]:
        """Получаем список записей с детальной информацией"""
        if not self.ensure_session():
            return []

        try:
            current_time = int(time.time())

            if from_time is None:
                from_time = current_time - 300
            if to_time is None:
                to_time = current_time

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "List",
                "version": self.api_version,
                "_sid": self.sid,
                "offset": "0",
                "limit": str(limit),
                "fromTime": str(from_time),
                "toTime": str(to_time),
                "blIncludeThumb": "true",
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
                        start_time = rec.get("startTime", current_time - 60)

                        if start_time <= 0 or start_time > current_time:
                            start_time = current_time - 60

                        recording = Recording(
                            id=str(rec.get("id")),
                            camera_id=str(rec.get("cameraId", "unknown")),
                            start_time=start_time,
                            duration=rec.get("duration", 0),
                            size=rec.get("size", 0),
                        )
                        recordings.append(recording)

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"📋 Запись {recording.id} добавлена")

                    except Exception as e:
                        logger.warning(
                            f"⚠️ Ошибка обработки записи {rec.get('id')}: {e}"
                        )
                        continue

                logger.debug(f"🎥 Получено {len(recordings)} записей за период")
                return recordings

            error_code = data.get("error", {}).get("code", "unknown")
            logger.warning(f"⚠️ Ошибка API (код {error_code}): {data}")
            return []

        except RequestException as e:
            logger.error(f"❌ Ошибка при получении записей: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def download_recording_fragment(
        self, recording_id: str, offset_ms: int, duration_ms: int = 10000
    ) -> Optional[str]:
        """Скачивает фрагмент записи"""
        if not self.ensure_session():
            return None

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                suffix=f"_{recording_id}_frag_{offset_ms}_{duration_ms}.mp4",
                delete=False,
                dir="/tmp",
            )
            temp_file.close()

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": self.api_version,
                "_sid": self.sid,
                "id": recording_id,
                "mountId": "0",
                "offsetTimeMs": str(offset_ms),
                "playTimeMs": str(duration_ms),
            }

            logger.info(
                f"📥 Скачиваю фрагмент записи {recording_id}: "
                f"смещение={offset_ms/1000:.1f}с, "
                f"длительность={duration_ms/1000:.1f}с"
            )

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=30
            )

            if response.status_code != 200:
                logger.debug(
                    f"⚠️ API вернул статус {response.status_code} для записи {recording_id}"
                )
                return None

            response.raise_for_status()

            downloaded = 0
            with open(temp_file.name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            file_size = os.path.getsize(temp_file.name)

            if file_size > 0:
                logger.info(
                    f"✅ Фрагмент записи скачан: "
                    f"{file_size/(1024*1024):.1f} МБ, "
                    f"смещение={offset_ms/1000:.1f}с"
                )
                return temp_file.name
            else:
                logger.warning(f"⚠️ Скачанный фрагмент пуст: {temp_file.name}")
                os.remove(temp_file.name)
                return None

        except RequestException as e:
            logger.error(f"❌ Ошибка скачивания фрагмента записи {recording_id}: {e}")
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.remove(temp_file.name)
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка при скачивании: {e}")
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.remove(temp_file.name)
                except:
                    pass
            return None

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


# ============================================================================
# Telegram Bot
# ============================================================================


class TelegramBot:
    """Клиент для отправки сообщений в Telegram"""

    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 МБ

    def __init__(self):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.bot_name = None

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
                self.bot_name = data["result"]["first_name"]
                logger.info(f"🤖 Бот {self.bot_name} подключен к Telegram")
            else:
                logger.error(f"❌ Ошибка Telegram API: {data}")

        except Exception as e:
            logger.error(f"❌ Не удалось подключиться к Telegram: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Отправляет текстовое сообщение в Telegram"""
        try:
            data = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}

            response = requests.post(
                f"{self.base_url}/sendMessage", json=data, timeout=10
            )

            if response.status_code == 200:
                return True
            else:
                logger.error(
                    f"❌ Ошибка отправки сообщения: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка отправки сообщения: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def send_video(self, video_path: str, caption: str = "") -> bool:
        """Отправляет видео в Telegram"""
        try:
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
                    f"{self.base_url}/sendVideo", files=files, data=data, timeout=120
                )

                if response.status_code != 200:
                    logger.error(
                        f"❌ Telegram API вернул ошибку: {response.status_code} - {response.text}"
                    )
                    return False

                result = response.json()

                if result.get("ok"):
                    logger.info(f"✅ Видео успешно отправлено в Telegram")
                    return True
                else:
                    logger.error(f"❌ Ошибка Telegram API: {result}")
                    return False

        except Exception as e:
            logger.error(f"❌ Ошибка отправки видео: {e}")
            return False


# ============================================================================
# Менеджер состояния
# ============================================================================


class StateManager:
    """Менеджер состояния приложения"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.state_file = Path(config.state_file)
        self.progress: Dict[str, FragmentProgress] = {}
        self.completed_ids: Set[str] = set()

        self.load_state()

    def load_state(self) -> None:
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)

                self.completed_ids = set(state.get("completed_ids", []))

                progress_data = state.get("progress", {})
                for rec_id, data in progress_data.items():
                    self.progress[rec_id] = FragmentProgress(
                        recording_id=rec_id,
                        next_offset_ms=data.get("next_offset_ms", 0),
                        fragments_sent=data.get("fragments_sent", 0),
                        last_attempt_time=data.get("last_attempt_time", 0),
                        consecutive_fails=data.get("consecutive_fails", 0),
                        is_completed=data.get("is_completed", False),
                        estimated_duration_ms=data.get("estimated_duration_ms", 0),
                        last_seen_time=data.get("last_seen_time", 0),
                        full_duration_checked=data.get("full_duration_checked", False),
                    )

                logger.info(
                    f"📂 Загружено состояние: {len(self.progress)} активных записей"
                )
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")

    def save_state(self) -> None:
        """Сохраняет состояние в файл"""
        try:
            state = {
                "completed_ids": list(self.completed_ids),
                "progress": {
                    rec_id: {
                        "next_offset_ms": prog.next_offset_ms,
                        "fragments_sent": prog.fragments_sent,
                        "last_attempt_time": prog.last_attempt_time,
                        "consecutive_fails": prog.consecutive_fails,
                        "is_completed": prog.is_completed,
                        "estimated_duration_ms": prog.estimated_duration_ms,
                        "last_seen_time": prog.last_seen_time,
                        "full_duration_checked": prog.full_duration_checked,
                    }
                    for rec_id, prog in self.progress.items()
                },
                "updated_at": datetime.now().isoformat(),
            }

            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            logger.debug(f"💾 Состояние сохранено")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")

    def is_completed(self, recording_id: str) -> bool:
        """Проверяет, была ли запись полностью обработана"""
        return recording_id in self.completed_ids or (
            recording_id in self.progress and self.progress[recording_id].is_completed
        )

    def get_or_create_progress(self, recording_id: str) -> FragmentProgress:
        """Получает или создает прогресс для записи"""
        if recording_id not in self.progress:
            self.progress[recording_id] = FragmentProgress(
                recording_id=recording_id, last_seen_time=time.time()
            )
            logger.info(f"🆕 Начинаю отслеживание записи {recording_id}")

        self.progress[recording_id].last_seen_time = time.time()
        return self.progress[recording_id]

    def mark_fragment_sent(
        self, recording_id: str, next_offset: int, actual_duration_ms: int
    ) -> None:
        """Отмечает успешную отправку фрагмента"""
        if recording_id in self.progress:
            self.progress[recording_id].next_offset_ms = next_offset
            self.progress[recording_id].fragments_sent += 1
            self.progress[recording_id].last_attempt_time = time.time()
            self.progress[recording_id].consecutive_fails = 0

            if self.progress[recording_id].estimated_duration_ms == 0:
                self.progress[recording_id].estimated_duration_ms = (
                    self.config.video_estimated_duration_ms
                )

            self.save_state()

    def mark_fragment_failed(self, recording_id: str) -> None:
        """Отмечает неудачную попытку отправки фрагмента"""
        if recording_id in self.progress:
            self.progress[recording_id].last_attempt_time = time.time()
            self.progress[recording_id].consecutive_fails += 1
            self.save_state()

    def mark_completed(self, recording_id: str) -> None:
        """Помечает запись как полностью обработанную"""
        if recording_id in self.progress:
            self.progress[recording_id].is_completed = True

        self.completed_ids.add(recording_id)
        self.save_state()
        logger.info(f"✅ Запись {recording_id} помечена как завершённая")

    def cleanup_old_records(self) -> None:
        """Очищает старые записи"""
        current_time = time.time()
        max_age = self.config.cleanup_max_age_hours * 3600

        old_records = [
            rec_id
            for rec_id, prog in self.progress.items()
            if current_time - prog.last_seen_time > max_age
        ]

        for rec_id in old_records:
            del self.progress[rec_id]

        if old_records:
            logger.info(f"🧹 Очищено {len(old_records)} старых записей")

        self.save_state()

    def get_active_recordings(self) -> List[str]:
        """Возвращает список активных записей"""
        return [
            rec_id for rec_id, prog in self.progress.items() if not prog.is_completed
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику"""
        active_count = len(self.get_active_recordings())
        total_fragments = sum(prog.fragments_sent for prog in self.progress.values())

        return {
            "active_recordings": active_count,
            "completed_recordings": len(self.completed_ids),
            "total_fragments_sent": total_fragments,
        }


# ============================================================================
# Основная логика
# ============================================================================


def format_fragment_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    offset_seconds: float,
    duration_seconds: float,
) -> str:
    """Форматирует подпись для фрагмента"""
    try:
        real_start_time = recording.start_time + offset_seconds
        start_datetime = datetime.fromtimestamp(real_start_time)
        end_seconds = offset_seconds + duration_seconds

        caption = (
            f"<b>🚨 Обнаружено движение (фрагмент {fragment_num})</b>\n\n"
            f"<b>📅 Дата:</b> {start_datetime.strftime('%d.%m.%Y')}\n"
            f"<b>🕐 Время:</b> {start_datetime.strftime('%H:%M:%S')}\n"
            f"<b>📷 Камера:</b> {camera_name}\n"
            f"<b>⏱️ Позиция:</b> {offset_seconds:.1f}-{end_seconds:.1f} сек\n"
            f"<b>📁 Фрагмент:</b> {fragment_num}\n"
            f"<b>🎬 Длительность фрагмента:</b> {duration_seconds:.1f} сек\n\n"
            f"<i>#surveillance #motion_detected</i>"
        )

        return caption
    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return f"🚨 Обнаружено движение\n📷 Камера: {camera_name}\nФрагмент: {fragment_num}"


def send_startup_message(
    bot: TelegramBot,
    camera_name: str,
    camera_id: str,
    tracker: StateManager,
    check_interval: int,
    fragment_duration: int,
) -> None:
    """Отправляет сообщение о запуске"""
    stats = tracker.get_stats()

    message = (
        f"<b>🟢 Бот запущен (модульная версия)</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {check_interval} сек\n"
        f"<b>⏱️ Длительность фрагмента:</b> {fragment_duration/1000} сек\n"
        f"<b>📊 Активных записей:</b> {stats['active_recordings']}\n"
        f"<b>📈 Завершённых записей:</b> {stats['completed_recordings']}\n"
        f"<b>📁 Всего фрагментов:</b> {stats['total_fragments_sent']}\n\n"
        f"<i>Бот использует ffprobe для определения длительности видео</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение о запуске отправлено")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение о запуске")


def process_recording_fragments(
    synology: SynologyAPI,
    telegram: TelegramBot,
    tracker: StateManager,
    recording: Recording,
    camera_name: str,
    fragment_duration_ms: int = 10000,
) -> bool:
    """Обрабатывает фрагменты записи"""
    progress = tracker.get_or_create_progress(recording.id)
    current_time = time.time()

    if progress.is_completed:
        logger.debug(f"⏭️ Запись {recording.id} уже завершена, пропускаю")
        return False

    time_since_last = current_time - progress.last_attempt_time

    if (
        progress.fragments_sent == 0
        or time_since_last >= (fragment_duration_ms / 1000) - 2
    ):

        if progress.fragments_sent == 0 and not progress.full_duration_checked:
            logger.info(f"📏 Начинаю обработку записи {recording.id}")
            progress.full_duration_checked = True
            progress.estimated_duration_ms = 30000

        download_duration = fragment_duration_ms

        if progress.estimated_duration_ms > 0:
            remaining_ms = progress.estimated_duration_ms - progress.next_offset_ms

            if remaining_ms <= 0:
                logger.info(f"⏹️ Достигнут конец записи {recording.id}")
                tracker.mark_completed(recording.id)
                return False

            if remaining_ms < fragment_duration_ms:
                download_duration = remaining_ms
                logger.info(
                    f"📏 Осталось {remaining_ms/1000:.1f} сек, скачиваю остаток"
                )

        fragment_file = synology.download_recording_fragment(
            recording.id, progress.next_offset_ms, int(download_duration)
        )

        if fragment_file:
            try:
                actual_duration, duration_success = get_video_duration(fragment_file)

                if not duration_success or actual_duration <= 0:
                    logger.warning(
                        f"⚠️ Не удалось определить длительность фрагмента {recording.id}, использую запрошенную"
                    )
                    actual_duration = download_duration / 1000

                caption = format_fragment_caption(
                    recording,
                    camera_name,
                    progress.fragments_sent + 1,
                    progress.next_offset_ms / 1000,
                    actual_duration,
                )

                if telegram.send_video(fragment_file, caption):
                    actual_duration_ms = int(actual_duration * 1000)
                    next_offset = progress.next_offset_ms + actual_duration_ms

                    tracker.mark_fragment_sent(
                        recording.id, next_offset, actual_duration_ms
                    )

                    logger.info(
                        f"✅ Отправлен фрагмент {progress.fragments_sent} записи {recording.id}: "
                        f"{progress.next_offset_ms/1000:.1f}-{next_offset/1000:.1f} сек "
                        f"(длительность: {actual_duration:.1f} сек)"
                    )

                    if progress.fragments_sent >= 3:
                        logger.info(
                            f"✅ Запись {recording.id} полностью обработана (3 фрагмента)"
                        )
                        tracker.mark_completed(recording.id)

                    return True
                else:
                    tracker.mark_fragment_failed(recording.id)
                    logger.error(
                        f"❌ Не удалось отправить фрагмент записи {recording.id}"
                    )

            finally:
                try:
                    os.remove(fragment_file)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")
        else:
            tracker.mark_fragment_failed(recording.id)
            logger.warning(f"⚠️ Не удалось скачать фрагмент записи {recording.id}")

            if progress.consecutive_fails >= 3:
                tracker.mark_completed(recording.id)
                logger.info(f"⏹️ Запись {recording.id} завершена (3 неудачных попытки)")

    return False


def main():
    """Основная функция"""
    logger.info("🚀 Запуск Surveillance Station Telegram Bot (модульная версия)")

    start_time = time.time()

    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Отсутствуют переменные: {missing_vars}")
        return

    config = AppConfig.from_env()

    synology = SynologyAPI()
    telegram = TelegramBot()
    tracker = StateManager(config)

    cameras = synology.get_cameras()
    camera_id = config.camera_id
    camera_name = synology.get_camera_name(camera_id)

    check_interval = config.check_interval
    fragment_duration_ms = config.fragment_duration_ms

    send_startup_message(
        telegram, camera_name, camera_id, tracker, check_interval, fragment_duration_ms
    )

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")
    logger.info(
        f"📹 Режим: разбивка на фрагменты по {fragment_duration_ms/1000} секунд"
    )
    logger.info(f"🔄 Интервал проверки: {check_interval} секунд")

    shutdown_requested = False
    fragments_sent_session = 0

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"🛑 Получен сигнал {signum}, завершаю работу...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("🔄 Начинаю мониторинг...")

    while not shutdown_requested:
        try:
            current_time = int(time.time())

            recordings = synology.get_recordings(
                camera_id=camera_id,
                limit=30,
                from_time=current_time - 600,
                to_time=current_time,
            )

            logger.debug(f"🔍 Найдено {len(recordings)} записей")

            for recording in recordings:
                if tracker.is_completed(recording.id):
                    continue

                if process_recording_fragments(
                    synology,
                    telegram,
                    tracker,
                    recording,
                    camera_name,
                    fragment_duration_ms,
                ):
                    fragments_sent_session += 1

            active_ids = tracker.get_active_recordings()
            if active_ids:
                logger.debug(f"🔍 Активные записи: {len(active_ids)} шт")

            for rec_id in active_ids:
                current_recording = None
                for rec in recordings:
                    if rec.id == rec_id:
                        current_recording = rec
                        break

                if current_recording:
                    if process_recording_fragments(
                        synology,
                        telegram,
                        tracker,
                        current_recording,
                        camera_name,
                        fragment_duration_ms,
                    ):
                        fragments_sent_session += 1
                else:
                    progress = tracker.progress.get(rec_id)
                    if progress and current_time - progress.last_seen_time > 60:
                        logger.info(
                            f"⏹️ Запись {rec_id} не найдена в списке, помечаю как завершённую"
                        )
                        tracker.mark_completed(rec_id)

            if int(time.time()) % 300 == 0:
                tracker.cleanup_old_records()
                stats = tracker.get_stats()
                logger.info(
                    f"📊 Статистика: {stats['active_recordings']} активных, "
                    f"{stats['completed_recordings']} завершённых, "
                    f"{stats['total_fragments_sent']} фрагментов"
                )

            for i in range(check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("🛑 Прерывание с клавиатуры")
            shutdown_requested = True
            break
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            time.sleep(10)

    session_duration = time.time() - start_time
    stats = tracker.get_stats()

    message = (
        f"<b>🔴 Бот остановлен</b>\n\n"
        f"<b>🤖 Бот:</b> {telegram.bot_name}\n"
        f"<b>⏱️ Время работы:</b> {session_duration:.1f} сек\n"
        f"<b>📊 Отправлено фрагментов:</b> {fragments_sent_session}\n"
        f"<b>📈 Активных записей:</b> {stats['active_recordings']}\n"
        f"<b>📊 Завершённых записей:</b> {stats['completed_recordings']}\n\n"
        f"<i>Бот завершил работу.</i>"
    )

    if telegram.send_message(message):
        logger.info("✅ Сообщение об остановке отправлено")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение об остановке")

    logger.info(f"👋 Завершение работы. Время: {session_duration:.1f} сек")
    logger.info(f"📊 Итог сессии: {fragments_sent_session} фрагментов")

    tracker.save_state()


if __name__ == "__main__":
    main()
