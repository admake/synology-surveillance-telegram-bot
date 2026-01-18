#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Версия с отправкой видеозаписей по частям в реальном времени
"""

import os
import json
import time
import signal
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Set
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
    duration: int  # Длительность в миллисекундах
    size: int  # Размер в байтах
    file_path: Optional[str] = None


@dataclass
class RecordingProgress:
    """Отслеживание прогресса отправки записи"""

    recording_id: str
    last_offset: int = 0  # Последнее отправленное смещение в миллисекундах
    last_processed_time: int = 0  # Время последней обработки
    fragments_sent: int = 0  # Количество отправленных фрагментов
    is_complete: bool = False  # Запись полностью отправлена


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
                from_time = current_time - 300  # 5 минут назад
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
                        start_time = rec.get("startTime", 0)

                        if start_time <= 0 or start_time > current_time:
                            filename = rec.get("filename", "")
                            if filename:
                                try:
                                    import re

                                    time_match = re.search(r"(\d{8})_(\d{6})", filename)
                                    if time_match:
                                        date_str = time_match.group(1)
                                        time_str = time_match.group(2)
                                        dt_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                                        dt = datetime.strptime(
                                            dt_str, "%Y-%m-%d %H:%M:%S"
                                        )
                                        start_time = int(dt.timestamp())
                                except:
                                    pass

                        if start_time <= 0 or start_time > current_time:
                            start_time = current_time - 300

                        duration = rec.get("duration", 10000)
                        size = rec.get("size", 0)

                        if size <= 0 and duration > 0:
                            size = int(duration / 1000 * 100 * 1024)

                        recording = Recording(
                            id=str(rec.get("id")),
                            camera_id=str(rec.get("cameraId", "unknown")),
                            start_time=start_time,
                            duration=duration,
                            size=size,
                        )
                        recordings.append(recording)

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"📋 Запись {recording.id}: "
                                f"время={datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}, "
                                f"длительность={duration}мс, размер={size} байт"
                            )

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
        self, recording: Recording, offset_ms: int, duration_ms: int = 10000
    ) -> Optional[str]:
        """Скачивает фрагмент записи с указанным смещением и длительностью"""
        if not self.ensure_session():
            return None

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                suffix=f"_{recording.id}_frag_{offset_ms}_{duration_ms}.mp4",
                delete=False,
                dir="/tmp",
            )
            temp_file.close()

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": self.api_version,
                "_sid": self.sid,
                "id": recording.id,
                "mountId": "0",
                "offsetTimeMs": str(offset_ms),
                "playTimeMs": str(duration_ms),
            }

            logger.debug(
                f"📥 Скачиваю фрагмент записи {recording.id}: "
                f"смещение={offset_ms/1000:.1f}с, "
                f"длительность={duration_ms/1000:.1f}с"
            )

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=60
            )
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(temp_file.name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            file_size = os.path.getsize(temp_file.name)

            if file_size > 0:
                logger.debug(
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
            logger.error(f"❌ Ошибка скачивания фрагмента записи {recording.id}: {e}")
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


class TelegramBot:
    """Клиент для отправки сообщений в Telegram"""

    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 МБ - лимит Telegram для видео

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
    def send_video(
        self, video_path: str, caption: str = "", part_info: str = ""
    ) -> bool:
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
                f"📤 Отправляю видео в Telegram ({file_size/(1024*1024):.1f} МБ) {part_info}"
            )

            with open(video_path, "rb") as video_file:
                files = {"video": video_file}
                data = {
                    "chat_id": self.chat_id,
                    "caption": caption,
                    "supports_streaming": True,
                    "parse_mode": "HTML",
                }

                if part_info:
                    if caption:
                        data["caption"] = f"{caption}\n\n{part_info}"
                    else:
                        data["caption"] = part_info

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
                    logger.info(f"✅ Видео успешно отправлено в Telegram {part_info}")
                    return True
                else:
                    logger.error(f"❌ Ошибка Telegram API: {result}")
                    return False

        except Exception as e:
            logger.error(f"❌ Ошибка отправки видео: {e}")
            return False


class RecordingTracker:
    """Отслеживает прогресс отправки записей"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.active_recordings: Dict[str, RecordingProgress] = (
            {}
        )  # ID записи -> прогресс
        self.completed_recordings: Set[str] = set()  # ID полностью отправленных записей

        self.load_state()

    def load_state(self) -> None:
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)

                    self.completed_recordings = set(
                        state.get("completed_recordings", [])
                    )

                    active_data = state.get("active_recordings", {})
                    for rec_id, rec_data in active_data.items():
                        progress = RecordingProgress(
                            recording_id=rec_id,
                            last_offset=rec_data.get("last_offset", 0),
                            last_processed_time=rec_data.get("last_processed_time", 0),
                            fragments_sent=rec_data.get("fragments_sent", 0),
                            is_complete=rec_data.get("is_complete", False),
                        )
                        self.active_recordings[rec_id] = progress

                    logger.info(
                        f"📂 Загружено состояние: {len(self.active_recordings)} активных записей, "
                        f"{len(self.completed_recordings)} завершённых"
                    )
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")

    def save_state(self) -> None:
        """Сохраняет состояние в файл"""
        try:
            state = {
                "completed_recordings": list(self.completed_recordings),
                "active_recordings": {
                    rec_id: {
                        "last_offset": progress.last_offset,
                        "last_processed_time": progress.last_processed_time,
                        "fragments_sent": progress.fragments_sent,
                        "is_complete": progress.is_complete,
                    }
                    for rec_id, progress in self.active_recordings.items()
                },
                "updated_at": datetime.now().isoformat(),
            }

            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            logger.debug(
                f"💾 Состояние сохранено: {len(self.active_recordings)} активных записей"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")

    def is_completed(self, recording_id: str) -> bool:
        """Проверяет, была ли запись полностью отправлена"""
        return recording_id in self.completed_recordings

    def get_progress(self, recording_id: str) -> Optional[RecordingProgress]:
        """Получает прогресс обработки записи"""
        return self.active_recordings.get(recording_id)

    def start_tracking(self, recording_id: str) -> RecordingProgress:
        """Начинает отслеживание новой записи"""
        progress = RecordingProgress(
            recording_id=recording_id,
            last_offset=0,
            last_processed_time=int(time.time()),
            fragments_sent=0,
            is_complete=False,
        )
        self.active_recordings[recording_id] = progress
        self.save_state()
        return progress

    def update_progress(self, recording_id: str, new_offset: int) -> None:
        """Обновляет прогресс отправки записи"""
        if recording_id in self.active_recordings:
            progress = self.active_recordings[recording_id]
            progress.last_offset = new_offset
            progress.last_processed_time = int(time.time())
            progress.fragments_sent += 1
            self.save_state()

    def mark_completed(self, recording_id: str) -> None:
        """Помечает запись как полностью отправленную"""
        if recording_id in self.active_recordings:
            del self.active_recordings[recording_id]
        self.completed_recordings.add(recording_id)
        self.save_state()

    def cleanup_old_records(self, max_age_hours: int = 24) -> None:
        """Очищает старые записи из состояния"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600

        # Удаляем старые активные записи
        old_active = [
            rec_id
            for rec_id, progress in self.active_recordings.items()
            if current_time - progress.last_processed_time > max_age_seconds
        ]

        for rec_id in old_active:
            del self.active_recordings[rec_id]

        if old_active:
            logger.info(f"🧹 Очищено {len(old_active)} старых активных записей")

        # Очищаем историю завершённых записей, если их слишком много
        if len(self.completed_recordings) > 1000:
            self.completed_recordings = set(list(self.completed_recordings)[-500:])
            logger.info("🧹 Очищены старые завершённые записи")

        self.save_state()


def format_duration(milliseconds: int) -> str:
    """Форматирует длительность в человекочитаемый вид"""
    seconds = milliseconds / 1000

    if seconds < 60:
        return f"{seconds:.1f} сек"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        if remaining_seconds > 0:
            return f"{minutes} мин {remaining_seconds:.0f} сек"
        return f"{minutes} мин"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        if minutes > 0:
            return f"{hours} ч {minutes} мин"
        return f"{hours} ч"


def format_fragment_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    total_fragments: int,
    offset_seconds: float,
    duration_seconds: float,
) -> str:
    """Форматирует подпись для фрагмента видео"""
    try:
        start_time = datetime.fromtimestamp(recording.start_time + offset_seconds)

        date_str = start_time.strftime("%d.%m.%Y")
        time_str = start_time.strftime("%H:%M:%S")

        caption = (
            f"<b>🚨 Обнаружено движение (фрагмент {fragment_num}/{total_fragments})</b>\n\n"
            f"<b>📅 Дата:</b> {date_str}\n"
            f"<b>🕐 Время:</b> {time_str}\n"
            f"<b>📷 Камера:</b> {camera_name}\n"
            f"<b>⏱️ Позиция:</b> {offset_seconds:.1f}-{offset_seconds + duration_seconds:.1f} сек\n"
            f"<b>📁 Фрагмент:</b> {fragment_num} из {total_fragments}\n\n"
            f"<i>#surveillance #motion_detected</i>"
        )

        return caption

    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return f"🚨 Обнаружено движение\n📷 Камера: {camera_name}\n📁 Фрагмент: {fragment_num}/{total_fragments}"


def send_startup_message(
    bot: TelegramBot,
    camera_name: str,
    camera_id: str,
    tracker: RecordingTracker,
    check_interval: int,
) -> None:
    """Отправляет сообщение о запуске контейнера"""
    message = (
        f"<b>🟢 Бот запущен (режим фрагментов)</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {check_interval} сек\n"
        f"<b>📊 Активных записей:</b> {len(tracker.active_recordings)}\n"
        f"<b>📈 Завершённых записей:</b> {len(tracker.completed_recordings)}\n\n"
        f"<i>Бот активен и отправляет видео фрагментами по 10 секунд...</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение о запуске отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение о запуске")


def send_shutdown_message(
    bot: TelegramBot,
    tracker: RecordingTracker,
    new_recordings: int,
    session_duration: float,
) -> None:
    """Отправляет сообщение об остановке контейнера"""
    duration_str = format_duration(int(session_duration * 1000))

    message = (
        f"<b>🔴 Бот остановлен</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>⏱️ Время работы:</b> {duration_str}\n"
        f"<b>📊 Обработано в этой сессии:</b> {new_recordings} новых записей\n"
        f"<b>📈 Активных записей:</b> {len(tracker.active_recordings)}\n"
        f"<b>📊 Завершённых записей:</b> {len(tracker.completed_recordings)}\n\n"
        f"<i>Бот завершил работу.</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение об остановке отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение об остановке")


def process_recording_fragments(
    synology: SynologyAPI,
    telegram: TelegramBot,
    tracker: RecordingTracker,
    recording: Recording,
    camera_name: str,
    fragment_duration_ms: int = 10000,
) -> bool:
    """Обрабатывает запись фрагментами"""
    progress = tracker.get_progress(recording.id)

    if not progress:
        # Начинаем новую запись
        progress = tracker.start_tracking(recording.id)
        logger.info(
            f"🆕 Начинаю обработку записи {recording.id}, длительность: {format_duration(recording.duration)}"
        )

    current_time = time.time()
    recording_age = current_time - (recording.start_time + recording.duration / 1000)

    # Определяем, нужно ли отправлять следующий фрагмент
    if progress.last_offset >= recording.duration:
        # Запись полностью отправлена
        tracker.mark_completed(recording.id)
        logger.info(
            f"✅ Запись {recording.id} полностью отправлена ({progress.fragments_sent} фрагментов)"
        )
        return True

    # Если запись старая (завершилась более 30 секунд назад) и мы отправили хотя бы один фрагмент
    if recording_age > 30 and progress.fragments_sent > 0:
        # Отправляем оставшуюся часть
        remaining_ms = recording.duration - progress.last_offset
        if remaining_ms > 0:
            fragment_ms = min(remaining_ms, fragment_duration_ms)

            # Скачиваем фрагмент
            fragment_file = synology.download_recording_fragment(
                recording, progress.last_offset, fragment_ms
            )

            if fragment_file:
                # Формируем подпись
                caption = format_fragment_caption(
                    recording,
                    camera_name,
                    progress.fragments_sent + 1,
                    progress.fragments_sent
                    + 1
                    + max(1, int(remaining_ms / fragment_duration_ms)),
                    progress.last_offset / 1000,
                    fragment_ms / 1000,
                )

                # Отправляем в Telegram
                if telegram.send_video(fragment_file, caption):
                    tracker.update_progress(
                        recording.id, progress.last_offset + fragment_ms
                    )
                    logger.info(
                        f"✅ Отправлен фрагмент {progress.fragments_sent} записи {recording.id}"
                    )
                else:
                    logger.error(
                        f"❌ Не удалось отправить фрагмент записи {recording.id}"
                    )

                # Удаляем временный файл
                try:
                    os.remove(fragment_file)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")

                return True
            else:
                logger.error(f"❌ Не удалось скачать фрагмент записи {recording.id}")
                return False

    # Для активных записей (которые ещё идут) отправляем следующий фрагмент
    # только если с момента последней отправки прошло достаточно времени
    time_since_last = current_time - progress.last_processed_time

    if time_since_last >= (fragment_duration_ms / 1000) - 2:  # -2 секунды для запаса
        # Определяем длительность фрагмента
        fragment_ms = fragment_duration_ms

        # Скачиваем фрагмент
        fragment_file = synology.download_recording_fragment(
            recording, progress.last_offset, fragment_ms
        )

        if fragment_file:
            # Формируем подпись
            total_estimated_fragments = max(
                1, int(recording.duration / fragment_duration_ms)
            )
            caption = format_fragment_caption(
                recording,
                camera_name,
                progress.fragments_sent + 1,
                total_estimated_fragments,
                progress.last_offset / 1000,
                fragment_ms / 1000,
            )

            # Отправляем в Telegram
            if telegram.send_video(fragment_file, caption):
                tracker.update_progress(
                    recording.id, progress.last_offset + fragment_ms
                )
                logger.info(
                    f"✅ Отправлен фрагмент {progress.fragments_sent} записи {recording.id}"
                )
            else:
                logger.error(f"❌ Не удалось отправить фрагмент записи {recording.id}")

            # Удаляем временный файл
            try:
                os.remove(fragment_file)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")

            return True
        else:
            logger.error(f"❌ Не удалось скачать фрагмент записи {recording.id}")
            return False

    return False


def main():
    """Основная функция приложения"""
    logger.info("🚀 Запуск Surveillance Station Telegram Bot (режим фрагментов)")

    os.environ["CONTAINER_START_TIME"] = datetime.now().isoformat()
    start_time = time.time()

    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Отсутствуют обязательные переменные: {missing_vars}")
        return

    synology = SynologyAPI()
    telegram = TelegramBot()
    tracker = RecordingTracker(os.getenv("STATE_FILE", "/data/state.json"))

    cameras = synology.get_cameras()
    camera_id = os.getenv("CAMERA_ID", "5")
    camera_name = synology.get_camera_name(camera_id)

    check_interval = int(os.getenv("CHECK_INTERVAL", "10"))
    fragment_duration_ms = int(os.getenv("FRAGMENT_DURATION_MS", "10000"))

    send_startup_message(telegram, camera_name, camera_id, tracker, check_interval)

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")
    logger.info(f"📹 Режим: отправка фрагментами по {fragment_duration_ms/1000} секунд")
    logger.info(f"🔄 Интервал проверки: {check_interval} секунд")

    shutdown_requested = False
    new_recordings_session = 0

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"🛑 Получен сигнал {signum}, завершаю работу...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("🔄 Начинаю мониторинг записей...")

    while not shutdown_requested:
        try:
            current_time = int(time.time())

            # Получаем записи за последние 5 минут
            recordings = synology.get_recordings(
                camera_id=camera_id,
                limit=20,
                from_time=current_time - 300,
                to_time=current_time,
            )

            if recordings:
                logger.debug(f"🔍 Найдено {len(recordings)} записей")

                # Обрабатываем записи от старых к новым
                for recording in recordings:
                    # Пропускаем завершённые записи
                    if tracker.is_completed(recording.id):
                        continue

                    # Обрабатываем запись фрагментами
                    if process_recording_fragments(
                        synology,
                        telegram,
                        tracker,
                        recording,
                        camera_name,
                        fragment_duration_ms,
                    ):
                        new_recordings_session += 1

            # Периодически очищаем старые записи
            if int(time.time()) % 300 == 0:  # Каждые 5 минут
                tracker.cleanup_old_records()
                logger.info(
                    f"📊 Статистика: {len(tracker.active_recordings)} активных записей, "
                    f"{len(tracker.completed_recordings)} завершённых"
                )

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
            time.sleep(10)

    session_duration = time.time() - start_time

    send_shutdown_message(telegram, tracker, new_recordings_session, session_duration)

    logger.info(
        f"👋 Завершение работы бота. Время работы: {session_duration:.1f} секунд"
    )
    logger.info(f"📊 Итог сессии: обработано {new_recordings_session} новых фрагментов")

    tracker.save_state()


if __name__ == "__main__":
    main()
