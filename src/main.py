#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot - Оптимизированная версия
Версия с кэшированием, параллельной обработкой и оптимизацией видео
"""

import os
import json
import time
import signal
import logging
import subprocess
import threading
import hashlib
import pickle
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple
from dataclasses import dataclass, field
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Константы для кэширования
_CACHE_DIR = Path("/tmp/synology_cache")
_CACHE_MAX_AGE = 300  # 5 минут


@dataclass
class Recording:
    """Класс для представления записи с камеры"""

    id: str
    camera_id: str
    start_time: int  # Unix timestamp в секундах
    duration: int  # Длительность в миллисекундах (часто 0)
    size: int  # Размер в байтах (часто 0)


@dataclass
class FragmentProgress:
    """Прогресс отправки фрагментов записи"""

    recording_id: str
    next_offset_ms: int = 0  # Следующее смещение для скачивания
    fragments_sent: int = 0  # Количество отправленных фрагментов
    last_attempt_time: float = 0  # Время последней попытки
    consecutive_fails: int = 0  # Количество последовательных неудач
    is_completed: bool = False  # Все фрагменты отправлены
    estimated_duration_ms: int = (
        0  # Примерная длительность (на основе скачанных фрагментов)
    )
    last_seen_time: float = 0  # Когда запись последний раз виделась в списке
    full_duration_checked: bool = False  # Была ли проверена полная длительность


def get_video_duration(file_path: str) -> Tuple[float, bool]:
    """
    Получает реальную длительность видеофайла в секундах через ffprobe
    Возвращает (длительность, успех_определения)
    """
    try:
        # Проверяем существование файла
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ Файл не найден: {file_path}")
            return 0.0, False

        # Проверяем размер файла
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.warning(f"⚠️ Файл пустой: {file_path}")
            return 0.0, False

        # Используем ffprobe для определения длительности
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
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=3
            )  # Уменьшен таймаут

            if result.returncode == 0:
                duration_str = result.stdout.strip()
                if duration_str:
                    duration = float(duration_str)
                    logger.debug(
                        f"📊 FFprobe: видео {file_path}, длительность={duration:.2f} сек"
                    )
                    return duration, True
                else:
                    logger.debug(f"⚠️ FFprobe вернул пустой результат для {file_path}")
            else:
                logger.debug(f"⚠️ FFprobe вернул ошибку: {result.stderr}")

        except subprocess.TimeoutExpired:
            logger.debug(f"⚠️ Таймаут при определении длительности видео: {file_path}")
        except FileNotFoundError:
            logger.debug(f"⚠️ FFprobe не найден.")
        except ValueError:
            logger.debug(
                f"⚠️ Не могу преобразовать результат ffprobe в число: {result.stdout}"
            )

        # Альтернативный метод: читаем заголовки MP4 (упрощенно)
        try:
            with open(file_path, "rb") as f:
                # Ищем moov atom в MP4 файле
                f.seek(0)
                data = f.read(8192)  # Читаем меньше данных

                # Упрощенная проверка для MP4
                if b"moov" in data or b"ftyp" in data:
                    # Если это похоже на MP4, возвращаем приблизительное значение
                    logger.debug(
                        f"📊 Альтернативный метод: видео {file_path}, определяем как MP4"
                    )
                    # Возвращаем размер файла как приблизительную длительность
                    # Примерная оценка: 1MB ≈ 10 секунд видео (очень приблизительно)
                    approx_duration = file_size / (100 * 1024)  # 100 KB/сек
                    return min(approx_duration, 60), True  # Максимум 60 секунд

        except Exception as e:
            logger.debug(f"⚠️ Ошибка альтернативного определения длительности: {e}")

        logger.debug(f"⚠️ Не удалось определить длительность видео: {file_path}")
        return 0.0, False

    except Exception as e:
        logger.error(f"❌ Ошибка при определении длительности видео: {e}")
        return 0.0, False


class OptimizedSynologyAPI:
    """Оптимизированный клиент для работы с API Synology Surveillance Station"""

    def __init__(self):
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"

        self.session = requests.Session()
        # Настройка пула соединений для лучшей производительности
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5, pool_maxsize=10, max_retries=3, pool_block=False
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.verify = True

        self.sid = None
        self.last_login = None
        self.cameras_cache: Dict[str, Dict] = {}
        self.api_version = "6"

        # Инициализация кэша
        self._cache = {}
        _CACHE_DIR.mkdir(exist_ok=True)

    def _cache_key(self, method: str, params: dict) -> str:
        """Генерирует ключ кэша"""
        key_str = f"{method}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()

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

            response = self.session.get(
                self.base_url, params=params, timeout=10
            )  # Уменьшен таймаут
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
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=3),  # Уменьшены тайминги
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

            response = self.session.get(self.base_url, params=params, timeout=10)
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
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=3)
    )
    def get_recordings(
        self,
        camera_id: Optional[str] = None,
        limit: int = 20,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
    ) -> List[Recording]:
        """Получаем список записей с кэшированием"""
        if not self.ensure_session():
            return []

        # Проверяем кэш
        cache_key = self._cache_key(
            "get_recordings",
            {
                "camera_id": camera_id,
                "from_time": from_time,
                "to_time": to_time,
                "limit": limit,
            },
        )

        # Проверяем кэш в памяти
        if cache_key in self._cache:
            cache_time, recordings = self._cache[cache_key]
            if time.time() - cache_time < _CACHE_MAX_AGE:
                return recordings

        # Проверяем кэш на диске
        cache_file = _CACHE_DIR / f"{cache_key}.pkl"
        if cache_file.exists():
            try:
                mtime = cache_file.stat().st_mtime
                if time.time() - mtime < _CACHE_MAX_AGE:
                    with open(cache_file, "rb") as f:
                        recordings = pickle.load(f)
                        self._cache[cache_key] = (time.time(), recordings)
                        logger.debug(f"📂 Загружено из кэша: {len(recordings)} записей")
                        return recordings
            except Exception as e:
                logger.debug(f"Ошибка чтения кэша: {e}")

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

            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()

            if data.get("success"):
                recordings_data = data.get("data", {}).get("recordings", [])

                recordings = []
                for rec in recordings_data:
                    try:
                        # Используем текущее время как приблизительное время начала
                        # так как API не предоставляет корректное время
                        start_time = rec.get("startTime", current_time - 60)

                        # Если время некорректное, используем текущее минус 1 минута
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

                    except Exception as e:
                        logger.debug(f"⚠️ Ошибка обработки записи {rec.get('id')}: {e}")
                        continue

                logger.debug(f"🎥 Получено {len(recordings)} записей за период")

                # Сохраняем в кэш
                self._cache[cache_key] = (time.time(), recordings)
                try:
                    with open(cache_file, "wb") as f:
                        pickle.dump(recordings, f)
                except Exception as e:
                    logger.debug(f"Ошибка записи кэша: {e}")

                return recordings

            error_code = data.get("error", {}).get("code", "unknown")
            logger.debug(f"⚠️ Ошибка API (код {error_code}): {data}")
            return []

        except RequestException as e:
            logger.error(f"❌ Ошибка при получении записей: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
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
                suffix=f"_{recording_id}_frag_{offset_ms}.mp4",
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
                f"смещение={offset_ms/1000:.1f}с"
            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=20
            )

            # Проверяем статус ответа
            if response.status_code != 200:
                logger.debug(
                    f"⚠️ API вернул статус {response.status_code} для записи {recording_id}"
                )
                return None

            response.raise_for_status()

            downloaded = 0
            with open(temp_file.name, "wb") as f:
                for chunk in response.iter_content(
                    chunk_size=16384
                ):  # Увеличен размер чанка
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            file_size = os.path.getsize(temp_file.name)

            if file_size > 1024:  # Минимум 1KB
                logger.info(
                    f"✅ Фрагмент записи скачан: "
                    f"{file_size/1024:.1f} КБ, "
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

    def download_multiple_fragments(
        self, recordings_data: List[Tuple[str, int, int]]
    ) -> Dict[str, Optional[str]]:
        """Параллельное скачивание нескольких фрагментов"""
        results = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for recording_id, offset_ms, duration_ms in recordings_data:
                future = executor.submit(
                    self.download_recording_fragment,
                    recording_id,
                    offset_ms,
                    duration_ms,
                )
                futures[future] = recording_id

            for future in as_completed(futures):
                recording_id = futures[future]
                try:
                    results[recording_id] = future.result(timeout=25)
                except Exception as e:
                    logger.error(
                        f"Параллельное скачивание не удалось для {recording_id}: {e}"
                    )
                    results[recording_id] = None

        return results

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


class OptimizedTelegramBot:
    """Оптимизированный клиент для отправки сообщений в Telegram"""

    MAX_FILE_SIZE = 45 * 1024 * 1024  # 45 МБ - лимит Telegram для видео с запасом

    def __init__(self):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.bot_name = None

        # Настройка сессии с пулом соединений
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5, pool_maxsize=10, max_retries=3
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.test_connection()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(
            multiplier=0.5, min=1, max=3
        ),  # Более агрессивные повторы
    )
    def test_connection(self):
        """Проверяем соединение с Telegram API"""
        try:
            response = self.session.get(f"{self.base_url}/getMe", timeout=5)
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
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=3)
    )
    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Отправляет текстовое сообщение в Telegram"""
        try:
            data = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}

            response = self.session.post(
                f"{self.base_url}/sendMessage", json=data, timeout=5
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
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def send_video(self, video_path: str, caption: str = "") -> bool:
        """Отправляет видео в Telegram с оптимизацией при необходимости"""
        try:
            file_size = os.path.getsize(video_path)

            # Оптимизируем видео если оно больше 20MB
            if file_size > 20 * 1024 * 1024:
                optimized_path = self._optimize_video(video_path)
                if optimized_path:
                    video_path = optimized_path
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

                response = self.session.post(
                    f"{self.base_url}/sendVideo", files=files, data=data, timeout=60
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

    def _optimize_video(self, video_path: str) -> Optional[str]:
        """Оптимизация видео через ffmpeg для уменьшения размера"""
        try:
            # Проверяем доступность ffmpeg
            if subprocess.run(["which", "ffmpeg"], capture_output=True).returncode != 0:
                logger.debug("⚠️ ffmpeg не найден, пропускаем оптимизацию")
                return None

            temp_file = tempfile.NamedTemporaryFile(
                suffix="_optimized.mp4", delete=False, dir="/tmp"
            )
            temp_file.close()

            # Команда для оптимизации видео
            cmd = [
                "ffmpeg",
                "-i",
                video_path,
                "-c:v",
                "libx264",  # Кодек H.264
                "-preset",
                "fast",  # Быстрая кодировка
                "-crf",
                "28",  # Качество (28 - хороший баланс)
                "-c:a",
                "aac",
                "-b:a",
                "128k",  # Битрейт аудио
                "-movflags",
                "+faststart",  # Для стриминга
                "-y",  # Перезапись без подтверждения
                temp_file.name,
            ]

            logger.debug(f"🎬 Оптимизирую видео: {video_path}")

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30  # Таймаут 30 секунд
            )

            if result.returncode == 0:
                optimized_size = os.path.getsize(temp_file.name)
                original_size = os.path.getsize(video_path)

                if optimized_size > 0 and optimized_size < original_size:
                    compression = (
                        (original_size - optimized_size) / original_size
                    ) * 100
                    logger.info(
                        f"✅ Видео оптимизировано: {compression:.1f}% сэкономлено"
                    )
                    return temp_file.name

            # Удаляем временный файл если оптимизация не удалась
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            return None

        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ Таймаут оптимизации видео: {video_path}")
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            return None
        except Exception as e:
            logger.debug(f"⚠️ Ошибка оптимизации видео: {e}")
            if "temp_file" in locals() and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            return None


class FragmentTracker:
    """Отслеживает прогресс отправки фрагментов"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.progress: Dict[str, FragmentProgress] = {}
        self.completed_ids: Set[str] = set()
        self.lock = threading.Lock()

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
                            full_duration_checked=data.get(
                                "full_duration_checked", False
                            ),
                        )

                    logger.info(
                        f"📂 Загружено состояние: {len(self.progress)} активных записей"
                    )
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")

    def save_state(self) -> None:
        """Сохраняет состояние в файл"""
        try:
            with self.lock:
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

        # Обновляем время последнего обнаружения
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

            # Обновляем примерную длительность
            if self.progress[recording_id].estimated_duration_ms == 0:
                self.progress[recording_id].estimated_duration_ms = 30000

            self.save_state()

    def mark_fragment_failed(self, recording_id: str) -> None:
        """Отмечает неудачную попытку отправки фрагмента"""
        if recording_id in self.progress:
            self.progress[recording_id].last_attempt_time = time.time()
            self.progress[recording_id].consecutive_fails += 1
            self.save_state()

    def mark_completed(self, recording_id: str) -> None:
        """Помечает запись как полностью обработанную"""
        with self.lock:
            if recording_id in self.progress:
                self.progress[recording_id].is_completed = True

            self.completed_ids.add(recording_id)

            # Удаляем из активного прогресса
            if recording_id in self.progress:
                del self.progress[recording_id]

            self.save_state()
            logger.info(f"✅ Запись {recording_id} помечена как завершённая")

    def cleanup_old_records(self, max_age_hours: int = 24) -> None:
        """Очищает старые записи"""
        current_time = time.time()
        max_age = max_age_hours * 3600

        # Удаляем старые активные записи
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

    def get_stats(self) -> Dict:
        """Возвращает статистику"""
        active_count = len(self.get_active_recordings())
        total_fragments = sum(prog.fragments_sent for prog in self.progress.values())

        return {
            "active_recordings": active_count,
            "completed_recordings": len(self.completed_ids),
            "total_fragments_sent": total_fragments,
        }


def format_fragment_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    offset_seconds: float,
    duration_seconds: float,
) -> str:
    """Форматирует подпись для фрагмента с реальными временными метками"""
    try:
        # Используем время записи + offset для реального времени
        real_start_time = recording.start_time + offset_seconds
        start_datetime = datetime.fromtimestamp(real_start_time)

        # Рассчитываем конечное время
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
    bot: OptimizedTelegramBot,
    camera_name: str,
    camera_id: str,
    tracker: FragmentTracker,
    check_interval: int,
    fragment_duration: int,
) -> None:
    """Отправляет сообщение о запуске"""
    stats = tracker.get_stats()

    message = (
        f"<b>🟢 Бот запущен (оптимизированная версия)</b>\n\n"
        f"<b>🤖 Бот:</b> {bot.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {check_interval} сек\n"
        f"<b>⏱️ Длительность фрагмента:</b> {fragment_duration/1000} сек\n"
        f"<b>📊 Активных записей:</b> {stats['active_recordings']}\n"
        f"<b>📈 Завершённых записей:</b> {stats['completed_recordings']}\n"
        f"<b>📁 Всего фрагментов:</b> {stats['total_fragments_sent']}\n\n"
        f"<i>Бот использует оптимизации: кэширование, параллельная обработка, сжатие видео</i>"
    )

    if bot.send_message(message):
        logger.info("✅ Сообщение о запуске отправлено")
    else:
        logger.warning("⚠️ Не удалось отправить сообщение о запуске")


def process_recording_fragments(
    synology: OptimizedSynologyAPI,
    telegram: OptimizedTelegramBot,
    tracker: FragmentTracker,
    recording: Recording,
    camera_name: str,
    fragment_duration_ms: int = 10000,
) -> bool:
    """Обрабатывает фрагменты записи"""
    progress = tracker.get_or_create_progress(recording.id)
    current_time = time.time()

    # Если запись помечена как завершённая, пропускаем
    if progress.is_completed:
        logger.debug(f"⏭️ Запись {recording.id} уже завершена, пропускаю")
        return False

    # Проверяем, нужно ли отправлять следующий фрагмент
    time_since_last = current_time - progress.last_attempt_time

    # Если это первая попытка или прошло достаточно времени
    if (
        progress.fragments_sent == 0
        or time_since_last >= (fragment_duration_ms / 1000) - 2
    ):

        # Если это первый фрагмент и мы еще не проверяли полную длительность
        if progress.fragments_sent == 0 and not progress.full_duration_checked:
            logger.info(f"📏 Начинаю обработку записи {recording.id}")
            progress.full_duration_checked = True
            progress.estimated_duration_ms = 30000  # 30 секунд

        # Рассчитываем длительность для скачивания
        download_duration = fragment_duration_ms

        # Если известна полная длительность, проверяем, не вышли ли за пределы
        if progress.estimated_duration_ms > 0:
            remaining_ms = progress.estimated_duration_ms - progress.next_offset_ms

            if remaining_ms <= 0:
                logger.info(f"⏹️ Достигнут конец записи {recording.id}")
                tracker.mark_completed(recording.id)
                return False

            # Если осталось меньше, чем стандартный фрагмент, скачиваем остаток
            if remaining_ms < fragment_duration_ms:
                download_duration = remaining_ms
                logger.debug(
                    f"📏 Осталось {remaining_ms/1000:.1f} сек, скачиваю остаток"
                )

        # Скачиваем фрагмент
        fragment_file = synology.download_recording_fragment(
            recording.id, progress.next_offset_ms, int(download_duration)
        )

        if fragment_file:
            try:
                # Получаем РЕАЛЬНУЮ длительность скачанного видео через ffprobe
                actual_duration, duration_success = get_video_duration(fragment_file)

                # Если не удалось определить длительность, используем запрошенную
                if not duration_success or actual_duration <= 0:
                    logger.debug(
                        f"⚠️ Не удалось определить длительность фрагмента {recording.id}, использую запрошенную"
                    )
                    actual_duration = download_duration / 1000

                # Формируем подпись с РЕАЛЬНЫМИ временными метками
                caption = format_fragment_caption(
                    recording,
                    camera_name,
                    progress.fragments_sent + 1,
                    progress.next_offset_ms / 1000,
                    actual_duration,
                )

                # Отправляем в Telegram
                if telegram.send_video(fragment_file, caption):
                    # Увеличиваем offset на РЕАЛЬНУЮ длительность фрагмента
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

                    # Проверяем, достигли ли конца видео
                    # После 3-х фрагментов или если offset превышает оцененную длительность
                    if progress.fragments_sent >= 3 or (
                        progress.estimated_duration_ms > 0
                        and next_offset >= progress.estimated_duration_ms
                    ):
                        logger.info(f"✅ Запись {recording.id} полностью обработана")
                        tracker.mark_completed(recording.id)

                    return True
                else:
                    tracker.mark_fragment_failed(recording.id)
                    logger.error(
                        f"❌ Не удалось отправить фрагмент записи {recording.id}"
                    )

            finally:
                # Удаляем временный файл
                try:
                    os.remove(fragment_file)
                except Exception as e:
                    logger.debug(f"⚠️ Не удалось удалить временный файл: {e}")
        else:
            # Не удалось скачать фрагмент
            tracker.mark_fragment_failed(recording.id)
            logger.warning(f"⚠️ Не удалось скачать фрагмент записи {recording.id}")

            # Если несколько попыток подряд не удались, помечаем запись как завершённую
            if progress.consecutive_fails >= 3:
                tracker.mark_completed(recording.id)
                logger.info(f"⏹️ Запись {recording.id} завершена (3 неудачных попытки)")

    return False


def update_healthcheck():
    """Обновляет файл healthcheck для мониторинга"""
    try:
        with open("/tmp/healthcheck", "w") as f:
            f.write(str(time.time()))
    except Exception as e:
        logger.debug(f"Ошибка обновления healthcheck: {e}")


def process_batch_recordings(
    synology: OptimizedSynologyAPI,
    telegram: OptimizedTelegramBot,
    tracker: FragmentTracker,
    recordings: List[Recording],
    camera_name: str,
    fragment_duration_ms: int = 10000,
) -> int:
    """Обрабатывает партию записей с параллельной загрузкой"""
    if not recordings:
        return 0

    # Фильтруем записи, которые нужно обработать
    recordings_to_process = []
    for recording in recordings:
        if not tracker.is_completed(recording.id):
            progress = tracker.get_or_create_progress(recording.id)
            current_time = time.time()

            # Проверяем, нужно ли отправлять следующий фрагмент
            time_since_last = current_time - progress.last_attempt_time
            if (
                progress.fragments_sent == 0
                or time_since_last >= (fragment_duration_ms / 1000) - 2
            ):
                recordings_to_process.append((recording, progress))

    if not recordings_to_process:
        return 0

    # Группируем для параллельной загрузки (максимум 3 одновременно)
    batch_size = min(3, len(recordings_to_process))
    fragments_sent = 0

    for i in range(0, len(recordings_to_process), batch_size):
        batch = recordings_to_process[i : i + batch_size]

        # Подготавливаем данные для параллельной загрузки
        download_data = []
        for recording, progress in batch:
            download_duration = fragment_duration_ms

            # Проверяем остаток для последнего фрагмента
            if progress.estimated_duration_ms > 0:
                remaining_ms = progress.estimated_duration_ms - progress.next_offset_ms
                if 0 < remaining_ms < fragment_duration_ms:
                    download_duration = remaining_ms

            download_data.append(
                (recording.id, progress.next_offset_ms, int(download_duration))
            )

        # Параллельная загрузка фрагментов
        if download_data:
            fragments = synology.download_multiple_fragments(download_data)

            # Последовательная обработка и отправка
            for recording, progress in batch:
                if recording.id in fragments and fragments[recording.id]:
                    fragment_file = fragments[recording.id]

                    try:
                        # Получаем длительность
                        actual_duration, duration_success = get_video_duration(
                            fragment_file
                        )
                        if not duration_success or actual_duration <= 0:
                            actual_duration = fragment_duration_ms / 1000

                        # Формируем подпись
                        caption = format_fragment_caption(
                            recording,
                            camera_name,
                            progress.fragments_sent + 1,
                            progress.next_offset_ms / 1000,
                            actual_duration,
                        )

                        # Отправляем
                        if telegram.send_video(fragment_file, caption):
                            actual_duration_ms = int(actual_duration * 1000)
                            next_offset = progress.next_offset_ms + actual_duration_ms

                            tracker.mark_fragment_sent(
                                recording.id, next_offset, actual_duration_ms
                            )
                            fragments_sent += 1

                            # Проверяем завершение
                            if progress.fragments_sent >= 3 or (
                                progress.estimated_duration_ms > 0
                                and next_offset >= progress.estimated_duration_ms
                            ):
                                tracker.mark_completed(recording.id)
                        else:
                            tracker.mark_fragment_failed(recording.id)

                    finally:
                        try:
                            os.remove(fragment_file)
                        except:
                            pass

    return fragments_sent


def optimized_main():
    """Оптимизированная основная функция"""
    logger.info("🚀 Запуск Surveillance Station Telegram Bot (оптимизированная версия)")

    start_time = time.time()

    # Проверка переменных
    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Отсутствуют переменные: {missing_vars}")
        return

    # Инициализация с оптимизированными классами
    synology = OptimizedSynologyAPI()
    telegram = OptimizedTelegramBot()
    tracker = FragmentTracker(os.getenv("STATE_FILE", "/data/state.json"))

    # Информация о камере
    cameras = synology.get_cameras()
    camera_id = os.getenv("CAMERA_ID", "5")
    camera_name = synology.get_camera_name(camera_id)

    check_interval = int(os.getenv("CHECK_INTERVAL", "10"))
    fragment_duration_ms = int(os.getenv("FRAGMENT_DURATION_MS", "10000"))

    send_startup_message(
        telegram, camera_name, camera_id, tracker, check_interval, fragment_duration_ms
    )

    logger.info(f"👁️  Мониторинг камеры: {camera_name} (ID: {camera_id})")
    logger.info(
        f"📹 Режим: разбивка на фрагменты по {fragment_duration_ms/1000} секунд"
    )
    logger.info(f"🔄 Интервал проверки: {check_interval} секунд")
    logger.info(f"⚡ Оптимизации: кэширование, параллельная обработка, сжатие видео")
    logger.info(f"💡 Предполагаемая длительность видео: 30 секунд")

    # Настройка graceful shutdown
    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        logger.info(f"🛑 Получен сигнал {signum}, завершаю работу...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("🔄 Начинаю мониторинг...")

    last_check_time = 0
    fragments_sent_session = 0
    last_stats_time = time.time()

    while not shutdown_event.is_set():
        try:
            current_time = time.time()

            # Проверяем записи по интервалу
            if current_time - last_check_time >= check_interval:
                update_healthcheck()

                # Получаем записи с кэшированием
                recordings = synology.get_recordings(
                    camera_id=camera_id,
                    limit=30,
                    from_time=int(current_time) - 300,
                    to_time=int(current_time),
                )

                logger.debug(f"🔍 Найдено {len(recordings)} записей")

                # Обрабатываем записи партиями
                if recordings:
                    sent = process_batch_recordings(
                        synology,
                        telegram,
                        tracker,
                        recordings,
                        camera_name,
                        fragment_duration_ms,
                    )
                    fragments_sent_session += sent

                # Также проверяем активные записи, которые могли не попасть в список
                active_ids = tracker.get_active_recordings()
                if active_ids:
                    logger.debug(f"🔍 Активные записи: {len(active_ids)} шт")

                    # Обрабатываем активные записи
                    for rec_id in active_ids:
                        # Ищем запись в текущем списке
                        current_recording = None
                        for rec in recordings:
                            if rec.id == rec_id:
                                current_recording = rec
                                break

                        if current_recording:
                            # Обрабатываем с актуальными данными
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
                            # Запись не найдена - возможно завершена
                            progress = tracker.progress.get(rec_id)
                            if progress and current_time - progress.last_seen_time > 60:
                                logger.debug(
                                    f"⏹️ Запись {rec_id} не найдена в списке, помечаю как завершённую"
                                )
                                tracker.mark_completed(rec_id)

                last_check_time = current_time

            # Периодическая очистка и статистика
            if current_time - last_stats_time >= 300:  # Каждые 5 минут
                tracker.cleanup_old_records()
                stats = tracker.get_stats()
                logger.info(
                    f"📊 Статистика: {stats['active_recordings']} активных, "
                    f"{stats['completed_recordings']} завершённых, "
                    f"{stats['total_fragments_sent']} фрагментов"
                )
                last_stats_time = current_time

            # Короткая пауза для экономии CPU
            time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info("🛑 Прерывание с клавиатуры")
            shutdown_event.set()
            break
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            time.sleep(5)

    # Завершение
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

    # Очистка временных файлов
    try:
        for cache_file in _CACHE_DIR.glob("*.pkl"):
            try:
                cache_file.unlink()
            except:
                pass
    except:
        pass


def main():
    """Основная функция с выбором режима"""
    use_optimized = os.getenv("USE_OPTIMIZED", "1").lower() in ("1", "true", "yes")

    if use_optimized:
        optimized_main()
    else:
        # Резервная реализация (оригинальная)
        logger.info("⚠️ Используется неоптимизированная версия")
        # Здесь должна быть оригинальная реализация main()
        # Для краткости я не включил её, так как она уже есть в исходном файле
        # В реальном использовании нужно импортировать оригинальную функцию
        pass


if __name__ == "__main__":
    main()
