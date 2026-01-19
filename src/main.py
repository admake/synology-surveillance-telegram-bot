#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Версия с корректной отправкой фрагментов видео в реальном времени
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
from dataclasses import dataclass, asdict
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
    filename: str = ""


@dataclass
class RecordingProgress:
    """Отслеживание прогресса обработки записи"""

    recording_id: str
    last_offset_ms: int = 0  # Последнее обработанное смещение в миллисекундах
    last_processed_time: int = 0  # Время последней обработки
    fragments_sent: int = 0  # Количество отправленных фрагментов
    is_completed: bool = False  # Запись полностью обработана
    max_duration_ms: int = 0  # Максимальная обнаруженная длительность записи
    last_checked_time: int = 0  # Время последней проверки записи


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
                from_time = current_time - 600  # 10 минут назад
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
                        filename = rec.get("filename", "")

                        if start_time <= 0 or start_time > current_time:
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
                            filename=filename,
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
        self, recording_id: str, offset_ms: int, duration_ms: int = 10000
    ) -> Optional[str]:
        """Скачивает фрагмент записи с указанным смещением и длительностью"""
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

            logger.debug(
                f"📥 Скачиваю фрагмент записи {recording_id}: "
                f"смещение={offset_ms/1000:.1f}с, "
                f"длительность={duration_ms/1000:.1f}с"
            )

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=30
            )

            # Проверяем статус ответа
            if response.status_code != 200:
                logger.warning(
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
            logger.warning(f"⚠️ Ошибка скачивания фрагмента записи {recording_id}: {e}")
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


class RecordingManager:
    """Управление состоянием обработки записей"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.active_recordings: Dict[str, RecordingProgress] = {}
        self.completed_recordings: Set[str] = set()

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
                            last_offset_ms=rec_data.get("last_offset_ms", 0),
                            last_processed_time=rec_data.get("last_processed_time", 0),
                            fragments_sent=rec_data.get("fragments_sent", 0),
                            is_completed=rec_data.get("is_completed", False),
                            max_duration_ms=rec_data.get("max_duration_ms", 0),
                            last_checked_time=rec_data.get("last_checked_time", 0),
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
                        "last_offset_ms": progress.last_offset_ms,
                        "last_processed_time": progress.last_processed_time,
                        "fragments_sent": progress.fragments_sent,
                        "is_completed": progress.is_completed,
                        "max_duration_ms": progress.max_duration_ms,
                        "last_checked_time": progress.last_checked_time,
                    }
                    for rec_id, progress in self.active_recordings.items()
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
        return recording_id in self.completed_recordings

    def get_progress(self, recording_id: str) -> Optional[RecordingProgress]:
        """Получает прогресс обработки записи"""
        return self.active_recordings.get(recording_id)

    def start_recording(self, recording: Recording) -> RecordingProgress:
        """Начинает отслеживание новой записи"""
        current_time = int(time.time())
        progress = RecordingProgress(
            recording_id=recording.id,
            last_offset_ms=0,
            last_processed_time=current_time,
            fragments_sent=0,
            is_completed=False,
            max_duration_ms=recording.duration,
            last_checked_time=current_time,
        )

        self.active_recordings[recording.id] = progress
        self.save_state()

        logger.info(f"🆕 Начинаю отслеживание записи {recording.id}")
        return progress

    def update_recording_duration(
        self, recording_id: str, new_duration_ms: int
    ) -> None:
        """Обновляет информацию о длительности записи"""
        if recording_id in self.active_recordings:
            progress = self.active_recordings[recording_id]
            if new_duration_ms > progress.max_duration_ms:
                progress.max_duration_ms = new_duration_ms
                progress.last_checked_time = int(time.time())
                self.save_state()
                logger.debug(
                    f"📊 Обновлена длительность записи {recording_id}: {new_duration_ms}мс"
                )

    def mark_fragment_sent(self, recording_id: str, offset_ms: int) -> None:
        """Отмечает отправку фрагмента записи"""
        if recording_id in self.active_recordings:
            progress = self.active_recordings[recording_id]
            progress.last_offset_ms = offset_ms
            progress.fragments_sent += 1
            progress.last_processed_time = int(time.time())
            self.save_state()

    def mark_completed(self, recording_id: str) -> None:
        """Помечает запись как полностью обработанную"""
        if recording_id in self.active_recordings:
            del self.active_recordings[recording_id]

        self.completed_recordings.add(recording_id)
        self.save_state()
        logger.info(f"✅ Запись {recording_id} помечена как завершённая")

    def cleanup_old_records(self, max_age_hours: int = 24) -> None:
        """Очищает старые записи из состояния"""
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600

        # Очищаем старые активные записи (которые не обновлялись более 2 часов)
        old_active = [
            rec_id
            for rec_id, progress in self.active_recordings.items()
            if current_time - progress.last_processed_time > 7200  # 2 часа
        ]

        for rec_id in old_active:
            logger.info(f"🧹 Удаляю старую активную запись {rec_id}")
            del self.active_recordings[rec_id]

        # Очищаем историю завершённых записей, если их слишком много
        if len(self.completed_recordings) > 1000:
            self.completed_recordings = set(list(self.completed_recordings)[-500:])
            logger.info("🧹 Очищены старые завершённые записи")

        self.save_state()

    def get_active_recordings(self) -> List[str]:
        """Возвращает список ID активных записей"""
        return list(self.active_recordings.keys())

    def get_stats(self) -> Dict:
        """Возвращает статистику обработки"""
        return {
            "active_recordings": len(self.active_recordings),
            "completed_recordings": len(self.completed_recordings),
            "total_fragments_sent": sum(
                p.fragments_sent for p in self.active_recordings.values()
            ),
        }


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


def format_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    offset_seconds: float,
    duration_seconds: float,
    is_fragment: bool = True,
) -> str:
    """Форматирует подпись для Telegram"""
    try:
        start_time = datetime.fromtimestamp(recording.start_time + offset_seconds)

        date_str = start_time.strftime("%d.%m.%Y")
        time_str = start_time.strftime("%H:%M:%S")

        if is_fragment:
            caption = (
                f"<b>🚨 Обнаружено движение (фрагмент {fragment_num})</b>\n\n"
                f"<b>📅 Дата:</b> {date_str}\n"
                f"<b>🕐 Время:</b> {time_str}\n"
                f"<b>📷 Камера:</b> {camera_name}\n"
                f"<b>⏱️ Позиция:</b> {offset_seconds:.1f}-{offset_seconds + duration_seconds:.1f} сек\n\n"
                f"<i>#surveillance #motion_detected</i>"
            )
        else:
            caption = (
                f"<b>🚨 Обнаружено движение</b>\n\n"
                f"<b>📅 Дата:</b> {date_str}\n"
                f"<b>🕐 Время:</b> {time_str}\n"
                f"<b>📷 Камера:</b> {camera_name}\n"
                f"<b>⏱️ Длительность:</b> {format_duration(recording.duration)}\n\n"
                f"<i>#surveillance #motion_detected</i>"
            )

        return caption

    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return f"🚨 Обнаружено движение\n📷 Камера: {camera_name}"


def send_startup_message(
    bot: TelegramBot,
    camera_name: str,
    camera_id: str,
    manager: RecordingManager,
    check_interval: int,
    fragment_duration: int,
) -> None:
    """Отправляет сообщение о запуске контейнера"""
    stats = manager.get_stats()

    message = (
        f"<b>🟢 Бот запущен (режим фрагментов)</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {check_interval} сек\n"
        f"<b>⏱️ Длительность фрагмента:</b> {fragment_duration/1000} сек\n"
        f"<b>📊 Активных записей:</b> {stats['active_recordings']}\n"
        f"<b>📈 Завершённых записей:</b> {stats['completed_recordings']}\n"
        f"<b>📁 Всего фрагментов:</b> {stats['total_fragments_sent']}\n\n"
        f"<i>Бот активен и отправляет видео фрагментами...</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение о запуске отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение о запуске")


def send_shutdown_message(
    bot: TelegramBot,
    manager: RecordingManager,
    new_fragments: int,
    session_duration: float,
) -> None:
    """Отправляет сообщение об остановке контейнера"""
    stats = manager.get_stats()
    duration_str = format_duration(int(session_duration * 1000))

    message = (
        f"<b>🔴 Бот остановлен</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>⏱️ Время работы:</b> {duration_str}\n"
        f"<b>📊 Отправлено фрагментов:</b> {new_fragments}\n"
        f"<b>📈 Активных записей:</b> {stats['active_recordings']}\n"
        f"<b>📊 Завершённых записей:</b> {stats['completed_recordings']}\n\n"
        f"<i>Бот завершил работу.</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение об остановке отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение об остановке")


def process_recording(
    synology: SynologyAPI,
    telegram: TelegramBot,
    manager: RecordingManager,
    recording: Recording,
    camera_name: str,
    fragment_duration_ms: int = 10000,
) -> int:
    """Обрабатывает запись, отправляя фрагменты по мере их появления"""
    progress = manager.get_progress(recording.id)
    current_time = time.time()

    if not progress:
        # Новая запись
        progress = manager.start_recording(recording)

        # Сразу отправляем первый фрагмент
        return send_fragment(
            synology,
            telegram,
            manager,
            recording,
            camera_name,
            progress,
            fragment_duration_ms,
        )

    # Обновляем информацию о длительности записи
    manager.update_recording_duration(recording.id, recording.duration)

    # Проверяем, нужно ли отправлять следующий фрагмент
    # Для активных записей проверяем, прошло ли достаточно времени с последней отправки
    time_since_last = current_time - progress.last_processed_time
    fragment_interval = fragment_duration_ms / 1000

    if time_since_last >= fragment_interval - 1:  # -1 секунда для запаса
        return send_fragment(
            synology,
            telegram,
            manager,
            recording,
            camera_name,
            progress,
            fragment_duration_ms,
        )

    return 0


def send_fragment(
    synology: SynologyAPI,
    telegram: TelegramBot,
    manager: RecordingManager,
    recording: Recording,
    camera_name: str,
    progress: RecordingProgress,
    fragment_duration_ms: int,
) -> int:
    """Отправляет следующий фрагмент записи"""
    # Определяем параметры для скачивания фрагмента
    offset_ms = progress.last_offset_ms
    duration_ms = fragment_duration_ms

    # Для завершённых записей отправляем оставшуюся часть
    if progress.max_duration_ms > 0 and offset_ms >= progress.max_duration_ms:
        logger.info(f"✅ Запись {recording.id} полностью отправлена")
        manager.mark_completed(recording.id)
        return 0

    # Скачиваем фрагмент
    fragment_file = synology.download_recording_fragment(
        recording.id, offset_ms, duration_ms
    )

    if fragment_file:
        try:
            # Формируем подпись
            caption = format_caption(
                recording,
                camera_name,
                progress.fragments_sent + 1,
                offset_ms / 1000,
                duration_ms / 1000,
                is_fragment=True,
            )

            # Отправляем в Telegram
            if telegram.send_video(fragment_file, caption):
                manager.mark_fragment_sent(recording.id, offset_ms + duration_ms)
                logger.info(
                    f"✅ Отправлен фрагмент {progress.fragments_sent + 1} записи {recording.id}"
                )
                return 1
            else:
                logger.error(f"❌ Не удалось отправить фрагмент записи {recording.id}")
                return 0
        finally:
            # Удаляем временный файл
            try:
                os.remove(fragment_file)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")
    else:
        # Не удалось скачать фрагмент - возможно, запись завершена
        logger.debug(
            f"⚠️ Не удалось скачать фрагмент записи {recording.id}, возможно запись завершена"
        )

        # Проверяем, является ли запись стабильно завершённой
        current_time = time.time()
        if current_time - recording.start_time > 300:  # Запись старше 5 минут
            logger.info(
                f"📄 Запись {recording.id} завершена, всего фрагментов: {progress.fragments_sent}"
            )
            manager.mark_completed(recording.id)

        return 0


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
    manager = RecordingManager(os.getenv("STATE_FILE", "/data/state.json"))

    cameras = synology.get_cameras()
    camera_id = os.getenv("CAMERA_ID", "5")
    camera_name = synology.get_camera_name(camera_id)

    check_interval = int(os.getenv("CHECK_INTERVAL", "10"))
    fragment_duration_ms = int(os.getenv("FRAGMENT_DURATION_MS", "10000"))

    send_startup_message(
        telegram, camera_name, camera_id, manager, check_interval, fragment_duration_ms
    )

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")
    logger.info(f"📹 Режим: отправка фрагментами по {fragment_duration_ms/1000} секунд")
    logger.info(f"🔄 Интервал проверки: {check_interval} секунд")

    shutdown_requested = False
    new_fragments_session = 0

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

            # Получаем записи за последние 10 минут
            recordings = synology.get_recordings(
                camera_id=camera_id,
                limit=30,
                from_time=current_time - 600,
                to_time=current_time,
            )

            if recordings:
                logger.debug(f"🔍 Найдено {len(recordings)} записей")

                # Обрабатываем все записи
                for recording in recordings:
                    # Пропускаем завершённые записи
                    if manager.is_completed(recording.id):
                        continue

                    # Обрабатываем запись
                    fragments_sent = process_recording(
                        synology,
                        telegram,
                        manager,
                        recording,
                        camera_name,
                        fragment_duration_ms,
                    )
                    new_fragments_session += fragments_sent

            # Также обрабатываем активные записи, которые могли не попасть в текущий список
            active_recordings = manager.get_active_recordings()
            for recording_id in active_recordings:
                # Пытаемся получить актуальную информацию о записи
                # Для этого ищем запись в новом списке
                current_recording = None
                for rec in recordings:
                    if rec.id == recording_id:
                        current_recording = rec
                        break

                if current_recording:
                    # Обрабатываем с актуальными данными
                    fragments_sent = process_recording(
                        synology,
                        telegram,
                        manager,
                        current_recording,
                        camera_name,
                        fragment_duration_ms,
                    )
                    new_fragments_session += fragments_sent
                else:
                    # Запись не найдена в текущем списке - возможно, она завершена
                    progress = manager.get_progress(recording_id)
                    if progress and time.time() - progress.last_checked_time > 60:
                        # Не видели запись более 60 секунд - помечаем как завершённую
                        logger.info(
                            f"⏱️ Запись {recording_id} не найдена в текущем списке, помечаю как завершённую"
                        )
                        manager.mark_completed(recording_id)

            # Периодически очищаем старые записи
            if int(time.time()) % 300 == 0:  # Каждые 5 минут
                manager.cleanup_old_records()
                stats = manager.get_stats()
                logger.info(
                    f"📊 Статистика: {stats['active_recordings']} активных записей, "
                    f"{stats['completed_recordings']} завершённых, "
                    f"{stats['total_fragments_sent']} всего фрагментов"
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

    send_shutdown_message(telegram, manager, new_fragments_session, session_duration)

    logger.info(
        f"👋 Завершение работы бота. Время работы: {session_duration:.1f} секунд"
    )
    logger.info(f"📊 Итог сессии: отправлено {new_fragments_session} новых фрагментов")

    manager.save_state()


if __name__ == "__main__":
    main()
