#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Исправленная версия с загрузкой полных видеозаписей
"""

import os
import json
import time
import signal
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
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
        self, camera_id: Optional[str] = None, limit: int = 10
    ) -> List[Recording]:
        """Получаем список записей с детальной информацией"""
        if not self.ensure_session():
            return []

        try:
            current_time = int(time.time())
            from_time = current_time - 3600
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

                logger.info(f"🎥 Получено {len(recordings)} записей за последний час")
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
    def download_complete_recording(self, recording: Recording) -> Optional[str]:
        """Скачивает запись целиком без обрезки"""
        if not self.ensure_session():
            return None

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                suffix=f"_{recording.id}.mp4", delete=False, dir="/tmp"
            )
            temp_file.close()

            # Ключевое изменение: НЕ передаем offsetTimeMs и playTimeMs
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": self.api_version,
                "_sid": self.sid,
                "id": recording.id,
                "mountId": "0",
                # Не передаем offsetTimeMs и playTimeMs - это загрузит полное видео
            }

            logger.info(f"📥 Скачиваю полную запись {recording.id}")

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=180
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

            # Пытаемся получить реальную длительность из метаданных файла
            actual_duration = self.get_video_duration(temp_file.name)
            if actual_duration:
                recording.duration = int(actual_duration * 1000)
                logger.info(
                    f"📏 Фактическая длительность видео: {actual_duration:.1f} сек"
                )

            recording.size = file_size

            logger.info(
                f"✅ Запись {recording.id} скачана: {file_size/(1024*1024):.1f} МБ"
            )

            return temp_file.name

        except RequestException as e:
            logger.error(f"❌ Ошибка скачивания записи {recording.id}: {e}")
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

    def get_video_duration(self, file_path: str) -> Optional[float]:
        """Получает длительность видео файла с помощью ffprobe"""
        try:
            if not os.path.exists(file_path):
                return None

            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    file_path,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0 and result.stdout:
                return float(result.stdout.strip())
        except Exception as e:
            logger.debug(f"Не удалось определить длительность видео: {e}")

        return None

    def handle_large_recording(self, recording: Recording) -> List[str]:
        """Обрабатывает большие записи, разбивая их на части для Telegram"""
        logger.info(
            f"📦 Обработка записи {recording.id} ({recording.duration/1000:.1f} сек)"
        )

        # Пытаемся скачать полную запись
        full_file = self.download_complete_recording(recording)

        if not full_file:
            return []

        file_size = os.path.getsize(full_file)

        # Если файл меньше лимита Telegram, возвращаем как есть
        if file_size <= 45 * 1024 * 1024:  # 45 МБ с запасом
            return [full_file]

        # Если файл слишком большой, разбиваем его
        logger.info(
            f"✂️ Файл слишком большой ({file_size/(1024*1024):.1f} МБ), разбиваю на части..."
        )

        try:
            # Получаем длительность видео
            total_duration = self.get_video_duration(full_file)
            if not total_duration or total_duration <= 0:
                logger.error("Не удалось получить длительность видео")
                return [full_file]

            # Вычисляем, на сколько частей нужно разбить
            # Предполагаем, что 1 минута видео ≈ 10 МБ
            estimated_size_per_minute = 10 * 1024 * 1024
            target_chunk_duration = (
                (45 * 1024 * 1024) / estimated_size_per_minute * 60
            )  # секунды

            if target_chunk_duration < 30:  # Минимум 30 секунд
                target_chunk_duration = 30

            num_chunks = int(total_duration / target_chunk_duration) + 1
            chunk_duration = total_duration / num_chunks

            logger.info(
                f"📊 Разбиваю на {num_chunks} частей по {chunk_duration:.1f} сек"
            )

            chunk_files = []

            # Разбиваем файл на части
            for i in range(num_chunks):
                start_time = i * chunk_duration
                chunk_file = tempfile.NamedTemporaryFile(
                    suffix=f"_{recording.id}_part_{i+1}_of_{num_chunks}.mp4",
                    delete=False,
                    dir="/tmp",
                )
                chunk_file.close()

                # Используем ffmpeg для вырезания части
                try:
                    subprocess.run(
                        [
                            "ffmpeg",
                            "-i",
                            full_file,
                            "-ss",
                            str(start_time),
                            "-t",
                            str(chunk_duration),
                            "-c",
                            "copy",  # Копируем кодек без перекодирования (быстро)
                            "-avoid_negative_ts",
                            "1",
                            chunk_file.name,
                        ],
                        capture_output=True,
                        timeout=30,
                        check=True,
                    )

                    chunk_size = os.path.getsize(chunk_file.name)
                    if chunk_size > 0:
                        chunk_files.append(chunk_file.name)
                        logger.info(
                            f"✅ Создана часть {i+1}/{num_chunks} ({chunk_size/(1024*1024):.1f} МБ)"
                        )
                    else:
                        logger.warning(f"⚠️ Созданная часть {i+1} пуста")
                        os.remove(chunk_file.name)

                except subprocess.CalledProcessError as e:
                    logger.error(f"❌ Ошибка при создании части {i+1}: {e}")
                    if os.path.exists(chunk_file.name):
                        os.remove(chunk_file.name)

            # Удаляем исходный большой файл
            os.remove(full_file)

            if chunk_files:
                return chunk_files
            else:
                logger.error("Не удалось создать ни одной части")
                return []

        except Exception as e:
            logger.error(f"❌ Ошибка при разбивке файла: {e}")
            return [full_file]  # Возвращаем как есть

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

    def send_video_chunks(
        self, recording: Recording, chunk_files: List[str], caption: str
    ) -> bool:
        """Отправляет видео частями в Telegram"""
        if not chunk_files:
            logger.error("❌ Нет файлов для отправки")
            return False

        total_parts = len(chunk_files)
        success_count = 0

        for i, chunk_file in enumerate(chunk_files):
            try:
                if i == 0:
                    # Первая часть с полным описанием
                    part_caption = caption
                else:
                    # Последующие части только с номером
                    part_caption = ""

                part_info = f"📁 Часть {i+1} из {total_parts}"

                if self.send_video(chunk_file, part_caption, part_info):
                    success_count += 1
                    logger.info(f"✅ Отправлена часть {i+1}/{total_parts}")
                else:
                    logger.error(f"❌ Не удалось отправить часть {i+1}/{total_parts}")

                # Удаляем временный файл
                try:
                    os.remove(chunk_file)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось удалить временный файл: {e}")

                # Пауза между отправками
                if i < len(chunk_files) - 1:
                    time.sleep(2)

            except Exception as e:
                logger.error(f"❌ Ошибка при отправке части {i+1}: {e}")
                # Пробуем удалить файл даже в случае ошибки
                try:
                    if os.path.exists(chunk_file):
                        os.remove(chunk_file)
                except:
                    pass

        logger.info(f"📊 Отправлено {success_count} из {total_parts} частей")
        return success_count > 0


class StateManager:
    """Управление состоянием обработанных записей"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.processed_ids = set()
        self.last_processed_time = 0
        self.total_processed = 0
        self.is_writable = True

        try:
            self.load_state()
        except PermissionError as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")
            logger.warning("⚠️ Состояние не будет сохраняться между запусками")
            self.is_writable = False
            self.last_processed_time = int(time.time() - 3600)

    def load_state(self) -> None:
        """Загружает состояние из файла"""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    self.processed_ids = set(state.get("processed_ids", []))
                    self.last_processed_time = state.get("last_processed_time", 0)
                    self.total_processed = state.get(
                        "total_processed", len(self.processed_ids)
                    )

                    logger.info(
                        f"📂 Загружено состояние: {len(self.processed_ids)} записей в памяти, "
                        f"{self.total_processed} всего обработано"
                    )

                    self.cleanup_old_records()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")
            self.last_processed_time = int(time.time() - 3600)

    def save_state(self) -> None:
        """Сохраняет состояние в файл"""
        if not self.is_writable:
            logger.debug("⚠️ Состояние не сохраняется (файл недоступен для записи)")
            return

        try:
            state = {
                "processed_ids": list(self.processed_ids),
                "last_processed_time": self.last_processed_time,
                "total_processed": self.total_processed,
                "updated_at": datetime.now().isoformat(),
                "container_started": os.getenv(
                    "CONTAINER_START_TIME", datetime.now().isoformat()
                ),
            }

            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

            logger.debug(
                f"💾 Состояние сохранено. Всего обработано: {self.total_processed}"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")
            self.is_writable = False

    def is_processed(self, recording_id: str) -> bool:
        """Проверяет, была ли запись обработана"""
        return recording_id in self.processed_ids

    def mark_processed(self, recording_id: str) -> None:
        """Помечает запись как обработанную"""
        self.processed_ids.add(recording_id)
        self.last_processed_time = int(time.time())
        self.total_processed += 1
        logger.debug(f"📝 Запись {recording_id} помечена как обработанная")
        self.save_state()

    def cleanup_old_records(self, max_age_days: int = 7) -> None:
        """Очищает старые записи из состояния"""
        if len(self.processed_ids) > 1000:
            self.processed_ids = set(list(self.processed_ids)[-1000:])
            logger.debug(
                f"🧹 Очищены старые записи, осталось: {len(self.processed_ids)}"
            )

    def get_stats(self) -> Dict:
        """Возвращает статистику обработки"""
        return {
            "processed_in_memory": len(self.processed_ids),
            "total_processed": self.total_processed,
            "last_processed_time": self.last_processed_time,
            "last_processed_human": (
                datetime.fromtimestamp(self.last_processed_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if self.last_processed_time > 0
                else "никогда"
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
    recording: Recording, camera_name: str, total_size_bytes: int = 0
) -> str:
    """Форматирует подпись для Telegram с корректной информацией"""
    try:
        start_time = datetime.fromtimestamp(recording.start_time)

        date_str = start_time.strftime("%d.%m.%Y")
        time_str = start_time.strftime("%H:%M:%S")

        duration_str = format_duration(recording.duration)

        if total_size_bytes > 0:
            if total_size_bytes < 1024 * 1024:
                size_str = f"{total_size_bytes/1024:.1f} KB"
            else:
                size_str = f"{total_size_bytes/(1024*1024):.1f} MB"
        else:
            size_str = "размер оценивается"

        caption = (
            f"<b>🚨 Обнаружено движение</b>\n\n"
            f"<b>📅 Дата:</b> {date_str}\n"
            f"<b>🕐 Время:</b> {time_str}\n"
            f"<b>📷 Камера:</b> {camera_name}\n"
            f"<b>⏱️ Длительность:</b> {duration_str}\n"
            f"<b>💾 Размер файла:</b> {size_str}\n\n"
        )

        # Добавляем информацию о полной записи
        if recording.duration > 120000:  # Если больше 2 минут
            caption += f"<i>📹 Отправляется полная запись события</i>\n"

        caption += f"<i>#surveillance #motion_detected</i>"

        return caption

    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return f"🚨 Обнаружено движение\n📷 Камера: {camera_name}"


def send_startup_message(
    bot: TelegramBot,
    camera_name: str,
    camera_id: str,
    state: StateManager,
    check_interval: int,
) -> None:
    """Отправляет сообщение о запуске контейнера"""
    stats = state.get_stats()

    message = (
        f"<b>🟢 Бот запущен</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {check_interval} сек\n"
        f"<b>📊 Всего обработано:</b> {stats['total_processed']} записей\n"
        f"<b>⏰ Последняя обработка:</b> {stats['last_processed_human']}\n"
        f"<b>📹 Режим:</b> Отправка полных записей\n\n"
        f"<i>Бот активен и мониторит события движения...</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение о запуске отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение о запуске")


def send_shutdown_message(
    bot: TelegramBot, state: StateManager, new_recordings: int, session_duration: float
) -> None:
    """Отправляет сообщение об остановке контейнера"""
    stats = state.get_stats()
    duration_str = format_duration(int(session_duration * 1000))

    message = (
        f"<b>🔴 Бот остановлен</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>⏱️ Время работы:</b> {duration_str}\n"
        f"<b>📊 Обработано в этой сессии:</b> {new_recordings} новых записей\n"
        f"<b>📈 Всего обработано:</b> {stats['total_processed']} записей\n\n"
        f"<i>Бот завершил работу.</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение об остановке отправлено в Telegram")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение об остановке")


def main():
    """Основная функция приложения"""
    logger.info("🚀 Запуск Surveillance Station Telegram Bot (режим полных записей)")

    os.environ["CONTAINER_START_TIME"] = datetime.now().isoformat()
    start_time = time.time()

    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Отсутствуют обязательные переменные: {missing_vars}")
        return

    synology = SynologyAPI()
    telegram = TelegramBot()
    state = StateManager(os.getenv("STATE_FILE", "/data/state.json"))

    cameras = synology.get_cameras()
    camera_id = os.getenv("CAMERA_ID", "5")
    camera_name = synology.get_camera_name(camera_id)

    check_interval = int(os.getenv("CHECK_INTERVAL", "30"))

    send_startup_message(telegram, camera_name, camera_id, state, check_interval)

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")
    logger.info("📹 Режим: отправка полных записей событий")

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
            recordings = synology.get_recordings(camera_id=camera_id, limit=20)
            pending_recordings = [r for r in recordings if not state.is_processed(r.id)]

            if pending_recordings:
                logger.info(
                    f"📋 Найдено {len(pending_recordings)} новых записей для обработки"
                )

                for recording in reversed(pending_recordings):
                    logger.info(
                        f"🆕 Обрабатываю запись {recording.id}, "
                        f"длительность: {format_duration(recording.duration)}"
                    )

                    try:
                        # Определяем стратегию загрузки
                        if recording.duration > 120000:  # Если больше 2 минут
                            # Используем обработчик больших записей
                            chunk_files = synology.handle_large_recording(recording)
                        else:
                            # Для коротких записей просто скачиваем целиком
                            full_file = synology.download_complete_recording(recording)
                            chunk_files = [full_file] if full_file else []

                        if chunk_files:
                            # Рассчитываем общий размер
                            total_size = sum(os.path.getsize(f) for f in chunk_files)

                            # Формируем подпись
                            caption = format_caption(recording, camera_name, total_size)

                            # Отправляем видео
                            logger.info(
                                f"📨 Отправляю запись {recording.id} ({len(chunk_files)} частей)..."
                            )
                            if telegram.send_video_chunks(
                                recording, chunk_files, caption
                            ):
                                state.mark_processed(recording.id)
                                new_recordings_session += 1
                                logger.info(
                                    f"✅ Запись {recording.id} успешно отправлена ({len(chunk_files)} частей)"
                                )
                            else:
                                logger.error(
                                    f"❌ Не удалось отправить запись {recording.id}"
                                )

                            # Очищаем временные файлы (если не удалены в send_video_chunks)
                            for chunk_file in chunk_files:
                                if os.path.exists(chunk_file):
                                    try:
                                        os.remove(chunk_file)
                                    except:
                                        pass
                        else:
                            logger.error(f"❌ Не удалось скачать запись {recording.id}")

                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки записи {recording.id}: {e}")

                    if shutdown_requested:
                        break

                logger.info(
                    f"📊 Обработка завершена. Обработано записей: {len(pending_recordings)}"
                )
            else:
                logger.debug("🔍 Новых записей не обнаружено")
                logger.info(
                    f"📊 Статистика: всего обработано {state.total_processed} записей"
                )

            state.save_state()

            logger.debug(f"⏳ Следующая проверка через {check_interval} секунд...")
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

    send_shutdown_message(telegram, state, new_recordings_session, session_duration)

    logger.info(
        f"👋 Завершение работы бота. Время работы: {session_duration:.1f} секунд"
    )
    logger.info(f"📊 Итог сессии: обработано {new_recordings_session} новых записей")

    state.save_state()


if __name__ == "__main__":
    main()
