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
import threading
import hashlib
import pickle
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple, Any, Union
from dataclasses import dataclass, field
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from requests.exceptions import RequestException

# ============================================================================
# Конфигурация
# ============================================================================


@dataclass
class AppConfig:
    """Конфигурация приложения"""

    # Общие настройки
    check_interval: int = 10
    fragment_duration_ms: int = 10000
    log_level: str = "INFO"
    state_file: str = "/data/state.json"
    use_optimized: bool = True
    camera_id: str = "5"

    # Настройки Synology
    syno_cache_max_age: int = 300  # 5 минут
    syno_max_workers: int = 3
    syno_timeout: int = 15

    # Настройки Telegram
    tg_max_file_size: int = 45 * 1024 * 1024  # 45 MB
    tg_optimize_threshold: int = 20 * 1024 * 1024  # 20 MB

    # Настройки видео
    video_estimated_duration_ms: int = 30000  # 30 секунд
    video_max_fragments: int = 3
    video_min_file_size: int = 1024  # 1 KB
    video_ffprobe_timeout: int = 3

    # Настройки обработки
    max_consecutive_fails: int = 3
    cleanup_max_age_hours: int = 24
    stats_interval: int = 300  # 5 минут

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Создает конфигурацию из переменных окружения"""
        config = cls()

        # Загружаем из env
        if os.getenv("CHECK_INTERVAL"):
            config.check_interval = int(os.getenv("CHECK_INTERVAL"))
        if os.getenv("FRAGMENT_DURATION_MS"):
            config.fragment_duration_ms = int(os.getenv("FRAGMENT_DURATION_MS"))

        config.log_level = os.getenv("LOG_LEVEL", config.log_level).upper()
        config.state_file = os.getenv("STATE_FILE", config.state_file)
        config.camera_id = os.getenv("CAMERA_ID", config.camera_id)

        use_optimized = os.getenv("USE_OPTIMIZED", "1").lower()
        config.use_optimized = use_optimized in ("1", "true", "yes")

        return config


# ============================================================================
# Модели данных
# ============================================================================


@dataclass
class Recording:
    """Класс для представления записи с камеры"""

    id: str
    camera_id: str
    start_time: int  # Unix timestamp в секундах
    duration: int  # Длительность в миллисекундах
    size: int  # Размер в байтах


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


@dataclass
class SessionStats:
    """Статистика сессии работы"""

    start_time: float = field(default_factory=time.time)
    fragments_sent: int = 0
    errors_count: int = 0

    @property
    def session_duration(self) -> float:
        """Длительность сессии в секундах"""
        return time.time() - self.start_time

    @property
    def uptime_hours(self) -> float:
        """Время работы в часах"""
        return self.session_duration / 3600


# ============================================================================
# Утилиты
# ============================================================================


class FileManager:
    """Менеджер для работы с файлами"""

    def __init__(self, temp_dir: str = "/tmp", cache_dir: str = "/tmp/synology_cache"):
        self.temp_dir = Path(temp_dir)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def temp_file(self, suffix: str = ".mp4", prefix: str = "temp") -> Path:
        """Контекстный менеджер для временных файлов"""
        timestamp = int(time.time())
        unique_id = uuid.uuid4().hex[:8]
        file_path = self.temp_dir / f"{prefix}_{timestamp}_{unique_id}{suffix}"

        try:
            yield file_path
        finally:
            self.safe_remove(file_path)

    def safe_remove(self, file_path: Union[str, Path]) -> bool:
        """Безопасно удаляет файл"""
        try:
            path = Path(file_path) if isinstance(file_path, str) else file_path
            if path.exists():
                path.unlink()
                return True
        except Exception:
            return False
        return False

    def get_cache_path(self, key: str) -> Path:
        """Возвращает путь к файлу кэша"""
        return self.cache_dir / f"{key}.pkl"

    def cleanup_old_temp_files(
        self, pattern: str = "*.mp4", max_age_seconds: int = 3600
    ):
        """Очищает старые временные файлы"""
        try:
            current_time = time.time()
            for file_path in self.temp_dir.glob(pattern):
                try:
                    if current_time - file_path.stat().st_mtime > max_age_seconds:
                        file_path.unlink()
                except Exception:
                    pass
        except Exception:
            pass


class StructuredLogger:
    """Структурированный логгер"""

    def __init__(self, name: str = __name__, level: str = "INFO"):
        self.logger = logging.getLogger(name)

        # Настройка формата
        formatter = logging.Formatter(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )

        # Настройка обработчика
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        # Настройка уровня
        self.logger.setLevel(getattr(logging, level))
        self.logger.handlers = [handler]

    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, **kwargs)

    def _log(self, level: int, message: str, **kwargs):
        if kwargs:
            extra_info = " ".join(f"{k}={v}" for k, v in kwargs.items())
            message = f"{message} [{extra_info}]"
        self.logger.log(level, message)


# ============================================================================
# Компоненты обработки видео
# ============================================================================


class VideoProcessor:
    """Обработчик видеофайлов"""

    def __init__(self, config: AppConfig):
        self.config = config

    def get_duration(self, file_path: str) -> Tuple[float, bool]:
        """Получает длительность видео файла через ffprobe"""
        try:
            if not os.path.exists(file_path):
                return 0.0, False

            file_size = os.path.getsize(file_path)
            if file_size < self.config.video_min_file_size:
                return 0.0, False

            # Используем ffprobe
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

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.video_ffprobe_timeout,
            )

            if result.returncode == 0 and result.stdout.strip():
                duration = float(result.stdout.strip())
                return duration, True

            # Альтернативный метод для MP4 файлов
            return self._estimate_duration_mp4(file_path), False

        except subprocess.TimeoutExpired:
            return 0.0, False
        except Exception:
            return 0.0, False

    def _estimate_duration_mp4(self, file_path: str) -> float:
        """Приблизительно оценивает длительность MP4 файла"""
        try:
            file_size = os.path.getsize(file_path)
            with open(file_path, "rb") as f:
                data = f.read(8192)

                if b"moov" in data or b"ftyp" in data:
                    # Приблизительная оценка: 1MB ≈ 10 секунд видео
                    approx_duration = file_size / (100 * 1024)
                    return min(approx_duration, 60)  # Максимум 60 секунд
        except Exception:
            pass
        return 0.0

    def optimize_if_needed(self, file_path: str) -> Optional[str]:
        """Оптимизирует видео если оно превышает порог"""
        try:
            file_size = os.path.getsize(file_path)
            if file_size <= self.config.tg_optimize_threshold:
                return None

            if subprocess.run(["which", "ffmpeg"], capture_output=True).returncode != 0:
                return None

            with tempfile.NamedTemporaryFile(
                suffix="_optimized.mp4", delete=False
            ) as temp:
                optimized_path = temp.name

            cmd = [
                "ffmpeg",
                "-i",
                file_path,
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "28",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                "-y",
                optimized_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                optimized_size = os.path.getsize(optimized_path)
                if 0 < optimized_size < file_size:
                    return optimized_path

            # Удаляем временный файл если оптимизация не удалась
            if os.path.exists(optimized_path):
                os.unlink(optimized_path)
            return None

        except Exception:
            return None


class FragmentSender:
    """Отправщик фрагментов в Telegram"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

        # Настройка сессии
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5, pool_maxsize=10, max_retries=3
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.bot_name = None
        self._test_connection()

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=3)
    )
    def _test_connection(self):
        """Проверяет соединение с Telegram API"""
        response = self.session.get(f"{self.base_url}/getMe", timeout=5)
        response.raise_for_status()

        data = response.json()
        if data.get("ok"):
            self.bot_name = data["result"]["first_name"]

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=3)
    )
    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Отправляет текстовое сообщение"""
        try:
            data = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode}
            response = self.session.post(
                f"{self.base_url}/sendMessage", json=data, timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5)
    )
    def send_video(self, video_path: str, caption: str = "") -> bool:
        """Отправляет видео файл"""
        try:
            file_size = os.path.getsize(video_path)
            if file_size > self.config.tg_max_file_size:
                return False

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

                return response.status_code == 200

        except Exception:
            return False

    def create_caption(
        self,
        recording: Recording,
        camera_name: str,
        fragment_num: int,
        offset_seconds: float,
        duration_seconds: float,
    ) -> str:
        """Создает подпись для фрагмента видео"""
        try:
            real_start_time = recording.start_time + offset_seconds
            start_datetime = datetime.fromtimestamp(real_start_time)
            end_seconds = offset_seconds + duration_seconds

            return (
                f"<b>🚨 Обнаружено движение (фрагмент {fragment_num})</b>\n\n"
                f"<b>📅 Дата:</b> {start_datetime.strftime('%d.%m.%Y')}\n"
                f"<b>🕐 Время:</b> {start_datetime.strftime('%H:%M:%S')}\n"
                f"<b>📷 Камера:</b> {camera_name}\n"
                f"<b>⏱️ Позиция:</b> {offset_seconds:.1f}-{end_seconds:.1f} сек\n"
                f"<b>📁 Фрагмент:</b> {fragment_num}\n"
                f"<b>🎬 Длительность:</b> {duration_seconds:.1f} сек"
            )
        except Exception:
            return f"🚨 Обнаружено движение\n📷 Камера: {camera_name}\nФрагмент: {fragment_num}"


# ============================================================================
# Компоненты работы с Synology API
# ============================================================================


class SynologyAPIClient:
    """Клиент для работы с API Synology Surveillance Station"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"

        self.session = requests.Session()
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

        # Кэширование
        self._cache = {}
        self.file_manager = FileManager()

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
        """Аутентификация в API"""
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

        response = self.session.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        if data.get("success"):
            self.sid = data["data"]["sid"]
            self.last_login = time.time()
            return True

        return False

    def ensure_session(self) -> bool:
        """Проверяет и обновляет сессию при необходимости"""
        if not self.sid or not self.last_login or (time.time() - self.last_login > 600):
            return self.login()
        return True

    def get_cameras(self) -> Dict[str, Dict]:
        """Получает список камер"""
        if not self.ensure_session():
            return {}

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
                    "name": cam.get("newName", cam.get("name", f'Камера {cam["id"]}')),
                    "ip": cam.get("ip", "N/A"),
                    "model": cam.get("model", "N/A"),
                }
                for cam in cameras
            }

            return self.cameras_cache

        return {}

    def get_recordings(
        self,
        camera_id: Optional[str] = None,
        limit: int = 20,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
    ) -> List[Recording]:
        """Получает список записей с кэшированием"""
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
            if time.time() - cache_time < self.config.syno_cache_max_age:
                return recordings

        # Проверяем кэш на диске
        cache_file = self.file_manager.get_cache_path(cache_key)
        if cache_file.exists():
            try:
                mtime = cache_file.stat().st_mtime
                if time.time() - mtime < self.config.syno_cache_max_age:
                    with open(cache_file, "rb") as f:
                        recordings = pickle.load(f)
                        self._cache[cache_key] = (time.time(), recordings)
                        return recordings
            except Exception:
                pass

        # Получаем свежие данные
        recordings = self._fetch_recordings(camera_id, limit, from_time, to_time)

        # Сохраняем в кэш
        self._cache[cache_key] = (time.time(), recordings)
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(recordings, f)
        except Exception:
            pass

        return recordings

    def _fetch_recordings(
        self,
        camera_id: Optional[str],
        limit: int,
        from_time: Optional[int],
        to_time: Optional[int],
    ) -> List[Recording]:
        """Получает записи с сервера"""
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

        response = self.session.get(self.base_url, params=params, timeout=15)
        response.raise_for_status()

        data = response.json()
        if not data.get("success"):
            return []

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
            except Exception:
                continue

        return recordings

    def download_recording_fragment(
        self, recording_id: str, offset_ms: int, duration_ms: int = 10000
    ) -> Optional[str]:
        """Скачивает фрагмент записи"""
        if not self.ensure_session():
            return None

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

        with self.file_manager.temp_file(
            suffix=f"_{recording_id}_frag_{offset_ms}.mp4", prefix="synology"
        ) as temp_path:

            try:
                response = self.session.get(
                    self.base_url, params=params, stream=True, timeout=20
                )

                if response.status_code != 200:
                    return None

                response.raise_for_status()

                with open(temp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)

                if temp_path.stat().st_size > self.config.video_min_file_size:
                    return str(temp_path)
                else:
                    return None

            except Exception:
                return None

    def download_multiple_fragments(
        self, recordings_data: List[Tuple[str, int, int]]
    ) -> Dict[str, Optional[str]]:
        """Параллельное скачивание нескольких фрагментов"""
        results = {}

        with ThreadPoolExecutor(max_workers=self.config.syno_max_workers) as executor:
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
                except Exception:
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


# ============================================================================
# Менеджер состояния
# ============================================================================


class StateManager:
    """Менеджер состояния приложения"""

    def __init__(self, config: AppConfig, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.state_file = Path(config.state_file)
        self.progress: Dict[str, FragmentProgress] = {}
        self.completed_ids: Set[str] = set()
        self.lock = threading.Lock()

        self.load_state()

    def load_state(self) -> None:
        """Загружает состояние из файла"""
        try:
            if not self.state_file.exists():
                return

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

            self.logger.info(
                "Состояние загружено",
                active_recordings=len(self.progress),
                completed_recordings=len(self.completed_ids),
            )

        except Exception as e:
            self.logger.warning("Не удалось загрузить состояние", error=str(e))

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

                self.logger.debug("Состояние сохранено")

        except Exception as e:
            self.logger.error("Ошибка сохранения состояния", error=str(e))

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

        self.progress[recording_id].last_seen_time = time.time()
        return self.progress[recording_id]

    def mark_fragment_sent(
        self, recording_id: str, next_offset: int, actual_duration_ms: int
    ) -> None:
        """Отмечает успешную отправку фрагмента"""
        if recording_id in self.progress:
            progress = self.progress[recording_id]
            progress.next_offset_ms = next_offset
            progress.fragments_sent += 1
            progress.last_attempt_time = time.time()
            progress.consecutive_fails = 0

            if progress.estimated_duration_ms == 0:
                progress.estimated_duration_ms = self.config.video_estimated_duration_ms

            self.save_state()

    def mark_fragment_failed(self, recording_id: str) -> None:
        """Отмечает неудачную попытку отправки фрагмента"""
        if recording_id in self.progress:
            progress = self.progress[recording_id]
            progress.last_attempt_time = time.time()
            progress.consecutive_fails += 1
            self.save_state()

    def mark_completed(self, recording_id: str) -> None:
        """Помечает запись как полностью обработанную"""
        with self.lock:
            if recording_id in self.progress:
                self.progress[recording_id].is_completed = True

            self.completed_ids.add(recording_id)

            if recording_id in self.progress:
                del self.progress[recording_id]

            self.save_state()
            self.logger.info(
                "Запись помечена как завершённая", recording_id=recording_id
            )

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
            self.logger.info("Очищены старые записи", count=len(old_records))

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
# Обработчик записей
# ============================================================================


class RecordingProcessor:
    """Обработчик записей с камер"""

    def __init__(
        self,
        synology: SynologyAPIClient,
        telegram: FragmentSender,
        state_manager: StateManager,
        video_processor: VideoProcessor,
        config: AppConfig,
        logger: StructuredLogger,
    ):
        self.synology = synology
        self.telegram = telegram
        self.state = state_manager
        self.video = video_processor
        self.config = config
        self.logger = logger
        self.file_manager = FileManager()

    def process_single_recording(self, recording: Recording, camera_name: str) -> bool:
        """Обрабатывает одну запись"""
        if self.state.is_completed(recording.id):
            return False

        progress = self.state.get_or_create_progress(recording.id)

        if not self._should_process_fragment(progress):
            return False

        return self._process_next_fragment(recording, progress, camera_name)

    def _should_process_fragment(self, progress: FragmentProgress) -> bool:
        """Определяет, нужно ли обрабатывать следующий фрагмент"""
        if progress.is_completed:
            return False

        current_time = time.time()
        time_since_last = current_time - progress.last_attempt_time
        fragment_interval = self.config.fragment_duration_ms / 1000 - 2

        return progress.fragments_sent == 0 or time_since_last >= fragment_interval

    def _process_next_fragment(
        self, recording: Recording, progress: FragmentProgress, camera_name: str
    ) -> bool:
        """Обрабатывает следующий фрагмент записи"""
        self._initialize_progress_if_needed(progress)

        download_duration = self._calculate_download_duration(progress)
        if download_duration <= 0:
            self.state.mark_completed(recording.id)
            return False

        # Скачиваем фрагмент
        fragment_file = self.synology.download_recording_fragment(
            recording.id, progress.next_offset_ms, download_duration
        )

        if not fragment_file:
            return self._handle_download_failure(recording.id, progress)

        # Обрабатываем скачанный фрагмент
        return self._process_downloaded_fragment(
            fragment_file, recording, progress, camera_name
        )

    def _initialize_progress_if_needed(self, progress: FragmentProgress):
        """Инициализирует прогресс если это первый фрагмент"""
        if progress.fragments_sent == 0 and not progress.full_duration_checked:
            progress.full_duration_checked = True
            progress.estimated_duration_ms = self.config.video_estimated_duration_ms

    def _calculate_download_duration(self, progress: FragmentProgress) -> int:
        """Рассчитывает длительность для скачивания"""
        if progress.estimated_duration_ms <= 0:
            return self.config.fragment_duration_ms

        remaining_ms = progress.estimated_duration_ms - progress.next_offset_ms

        if remaining_ms <= 0:
            return 0

        if remaining_ms < self.config.fragment_duration_ms:
            return remaining_ms

        return self.config.fragment_duration_ms

    def _handle_download_failure(
        self, recording_id: str, progress: FragmentProgress
    ) -> bool:
        """Обрабатывает неудачную загрузку"""
        self.state.mark_fragment_failed(recording_id)

        if progress.consecutive_fails >= self.config.max_consecutive_fails:
            self.state.mark_completed(recording_id)
            self.logger.info(
                "Запись завершена из-за неудачных попыток",
                recording_id=recording_id,
                attempts=progress.consecutive_fails,
            )

        return False

    def _process_downloaded_fragment(
        self,
        fragment_file: str,
        recording: Recording,
        progress: FragmentProgress,
        camera_name: str,
    ) -> bool:
        """Обрабатывает скачанный фрагмент"""
        try:
            # Получаем длительность видео
            actual_duration, duration_success = self.video.get_duration(fragment_file)
            if not duration_success or actual_duration <= 0:
                actual_duration = self.config.fragment_duration_ms / 1000

            # Оптимизируем видео при необходимости
            optimized_file = self.video.optimize_if_needed(fragment_file)
            if optimized_file:
                fragment_file = optimized_file

            # Создаем подпись
            caption = self.telegram.create_caption(
                recording,
                camera_name,
                progress.fragments_sent + 1,
                progress.next_offset_ms / 1000,
                actual_duration,
            )

            # Отправляем в Telegram
            if self.telegram.send_video(fragment_file, caption):
                actual_duration_ms = int(actual_duration * 1000)
                next_offset = progress.next_offset_ms + actual_duration_ms

                self.state.mark_fragment_sent(
                    recording.id, next_offset, actual_duration_ms
                )

                self.logger.info(
                    "Фрагмент отправлен",
                    recording_id=recording.id,
                    fragment_num=progress.fragments_sent,
                    offset=progress.next_offset_ms / 1000,
                    duration=actual_duration,
                )

                # Проверяем завершение
                if progress.fragments_sent >= self.config.video_max_fragments or (
                    progress.estimated_duration_ms > 0
                    and next_offset >= progress.estimated_duration_ms
                ):
                    self.state.mark_completed(recording.id)
                    self.logger.info(
                        "Запись полностью обработана", recording_id=recording.id
                    )

                return True
            else:
                self.state.mark_fragment_failed(recording.id)
                return False

        finally:
            # Очищаем временные файлы
            self.file_manager.safe_remove(fragment_file)
            if "optimized_file" in locals() and optimized_file:
                self.file_manager.safe_remove(optimized_file)

    def process_batch_recordings(
        self, recordings: List[Recording], camera_name: str
    ) -> int:
        """Обрабатывает партию записей с параллельной загрузкой"""
        if not recordings:
            return 0

        # Фильтруем записи для обработки
        recordings_to_process = []
        for recording in recordings:
            if not self.state.is_completed(recording.id):
                progress = self.state.get_or_create_progress(recording.id)
                if self._should_process_fragment(progress):
                    recordings_to_process.append((recording, progress))

        if not recordings_to_process:
            return 0

        fragments_sent = 0
        batch_size = min(self.config.syno_max_workers, len(recordings_to_process))

        for i in range(0, len(recordings_to_process), batch_size):
            batch = recordings_to_process[i : i + batch_size]
            fragments_sent += self._process_batch(batch, camera_name)

        return fragments_sent

    def _process_batch(
        self, batch: List[Tuple[Recording, FragmentProgress]], camera_name: str
    ) -> int:
        """Обрабатывает пакет записей"""
        # Подготавливаем данные для параллельной загрузки
        download_data = []
        for recording, progress in batch:
            download_duration = self._calculate_download_duration(progress)
            if download_duration > 0:
                download_data.append(
                    (recording.id, progress.next_offset_ms, download_duration)
                )

        if not download_data:
            return 0

        # Параллельная загрузка
        fragments = self.synology.download_multiple_fragments(download_data)

        # Обработка скачанных фрагментов
        fragments_sent = 0
        for recording, progress in batch:
            if recording.id in fragments and fragments[recording.id]:
                fragment_file = fragments[recording.id]
                if self._process_downloaded_fragment(
                    fragment_file, recording, progress, camera_name
                ):
                    fragments_sent += 1

        return fragments_sent


# ============================================================================
# Основное приложение
# ============================================================================


class SurveillanceBotApp:
    """Основное приложение бота"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = StructuredLogger(__name__, config.log_level)
        self.file_manager = FileManager()

        # Компоненты
        self.synology = None
        self.telegram = None
        self.state_manager = None
        self.video_processor = None
        self.recording_processor = None

        # Статистика
        self.session_stats = SessionStats()
        self.shutdown_event = threading.Event()
        self.camera_name = None

    def setup(self):
        """Настройка приложения"""
        self.logger.info("Настройка приложения")

        # Проверка переменных окружения
        self._validate_environment()

        # Инициализация компонентов
        self.synology = SynologyAPIClient(self.config)
        self.telegram = FragmentSender(self.config)
        self.state_manager = StateManager(self.config, self.logger)
        self.video_processor = VideoProcessor(self.config)

        # Получение информации о камере
        cameras = self.synology.get_cameras()
        self.camera_name = self.synology.get_camera_name(self.config.camera_id)

        # Инициализация процессора записей
        self.recording_processor = RecordingProcessor(
            self.synology,
            self.telegram,
            self.state_manager,
            self.video_processor,
            self.config,
            self.logger,
        )

        # Настройка обработчиков сигналов
        self._setup_signal_handlers()

        # Отправка сообщения о запуске
        self._send_startup_message()

        self.logger.info(
            "Приложение настроено",
            camera_name=self.camera_name,
            camera_id=self.config.camera_id,
            check_interval=self.config.check_interval,
        )

    def _validate_environment(self):
        """Проверяет обязательные переменные окружения"""
        required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(f"Отсутствуют переменные: {missing_vars}")

    def _setup_signal_handlers(self):
        """Настраивает обработчики сигналов"""

        def signal_handler(signum, frame):
            self.logger.info(f"Получен сигнал {signum}, завершаю работу...")
            self.shutdown_event.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _send_startup_message(self):
        """Отправляет сообщение о запуске"""
        stats = self.state_manager.get_stats()

        message = (
            f"<b>🟢 Бот запущен</b>\n\n"
            f"<b>🤖 Бот:</b> {self.telegram.bot_name}\n"
            f"<b>📷 Камера:</b> {self.camera_name} (ID: {self.config.camera_id})\n"
            f"<b>🔄 Интервал проверки:</b> {self.config.check_interval} сек\n"
            f"<b>⏱️ Длительность фрагмента:</b> {self.config.fragment_duration_ms/1000} сек\n"
            f"<b>📊 Активных записей:</b> {stats['active_recordings']}\n"
            f"<b>📈 Завершённых записей:</b> {stats['completed_recordings']}\n"
            f"<b>📁 Всего фрагментов:</b> {stats['total_fragments_sent']}"
        )

        if self.telegram.send_message(message):
            self.logger.info("Сообщение о запуске отправлено")
        else:
            self.logger.warning("Не удалось отправить сообщение о запуске")

    def run(self):
        """Запуск основного цикла"""
        self.logger.info("Начинаю мониторинг")

        last_check_time = 0
        last_stats_time = time.time()
        last_cleanup_time = time.time()

        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()

                # Проверка записей по интервалу
                if current_time - last_check_time >= self.config.check_interval:
                    self._process_check_cycle()
                    last_check_time = current_time

                # Статистика
                if current_time - last_stats_time >= self.config.stats_interval:
                    self._log_statistics()
                    last_stats_time = current_time

                # Очистка
                if current_time - last_cleanup_time >= 3600:  # Каждый час
                    self.file_manager.cleanup_old_temp_files()
                    last_cleanup_time = current_time

                # Короткая пауза
                time.sleep(0.5)

            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error("Ошибка в основном цикле", error=str(e))
                time.sleep(5)

    def _process_check_cycle(self):
        """Обрабатывает один цикл проверки"""
        # Обновление healthcheck
        try:
            with open("/tmp/healthcheck", "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass

        # Получение записей
        current_time = int(time.time())
        recordings = self.synology.get_recordings(
            camera_id=self.config.camera_id,
            limit=30,
            from_time=current_time - 300,
            to_time=current_time,
        )

        self.logger.debug("Найдено записей", count=len(recordings))

        # Обработка записей
        if recordings:
            sent = self.recording_processor.process_batch_recordings(
                recordings, self.camera_name
            )
            self.session_stats.fragments_sent += sent

        # Обработка активных записей
        self._process_active_recordings(recordings)

    def _process_active_recordings(self, current_recordings: List[Recording]):
        """Обрабатывает активные записи, которые могли не попасть в список"""
        active_ids = self.state_manager.get_active_recordings()

        for rec_id in active_ids:
            # Ищем запись в текущем списке
            current_recording = None
            for rec in current_recordings:
                if rec.id == rec_id:
                    current_recording = rec
                    break

            if current_recording:
                # Обрабатываем с актуальными данными
                if self.recording_processor.process_single_recording(
                    current_recording, self.camera_name
                ):
                    self.session_stats.fragments_sent += 1
            else:
                # Запись не найдена - возможно завершена
                progress = self.state_manager.progress.get(rec_id)
                if progress and time.time() - progress.last_seen_time > 60:
                    self.logger.debug(
                        "Запись не найдена, помечаю как завершённую",
                        recording_id=rec_id,
                    )
                    self.state_manager.mark_completed(rec_id)

    def _log_statistics(self):
        """Логирует статистику"""
        self.state_manager.cleanup_old_records()
        stats = self.state_manager.get_stats()

        self.logger.info(
            "Статистика",
            active_recordings=stats["active_recordings"],
            completed_recordings=stats["completed_recordings"],
            total_fragments=stats["total_fragments_sent"],
            session_fragments=self.session_stats.fragments_sent,
            session_uptime_hours=round(self.session_stats.uptime_hours, 1),
            errors_count=self.session_stats.errors_count,
        )

    def shutdown(self):
        """Корректное завершение работы"""
        self.logger.info("Завершение работы")

        # Отправка сообщения об остановке
        self._send_shutdown_message()

        # Сохранение состояния
        self.state_manager.save_state()

        # Очистка временных файлов
        self.file_manager.cleanup_old_temp_files()

        self.logger.info(
            "Работа завершена",
            session_duration=round(self.session_stats.session_duration, 1),
            fragments_sent=self.session_stats.fragments_sent,
        )

    def _send_shutdown_message(self):
        """Отправляет сообщение об остановке"""
        stats = self.state_manager.get_stats()

        message = (
            f"<b>🔴 Бот остановлен</b>\n\n"
            f"<b>🤖 Бот:</b> {self.telegram.bot_name}\n"
            f"<b>⏱️ Время работы:</b> {self.session_stats.session_duration:.1f} сек\n"
            f"<b>📊 Отправлено фрагментов:</b> {self.session_stats.fragments_sent}\n"
            f"<b>📈 Активных записей:</b> {stats['active_recordings']}\n"
            f"<b>📊 Завершённых записей:</b> {stats['completed_recordings']}"
        )

        if self.telegram.send_message(message):
            self.logger.info("Сообщение об остановке отправлено")
        else:
            self.logger.warning("Не удалось отправить сообщение об остановке")


def main():
    """Точка входа в приложение"""
    # Настройка конфигурации
    config = AppConfig.from_env()

    # Создание и запуск приложения
    app = SurveillanceBotApp(config)

    try:
        app.setup()
        app.run()
    except KeyboardInterrupt:
        app.logger.info("Прерывание с клавиатуры")
    except Exception as e:
        app.logger.error("Критическая ошибка", error=str(e))
        raise
    finally:
        app.shutdown()


if __name__ == "__main__":
    main()
