#!/usr/bin/env python3
"""Surveillance Station → Telegram Bot"""

import os
import json
import time
import signal
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple
from dataclasses import dataclass
import tempfile

import requests
import urllib3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import RequestException

# ============================================================================
# Logging
# ============================================================================

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Config
# ============================================================================


@dataclass
class AppConfig:
    check_interval: int = 30
    fragment_duration_ms: int = 10000
    state_file: str = "/data/state.json"
    camera_id: str = "1"
    lookback_minutes: int = 5
    max_consecutive_fails: int = 3
    cleanup_max_age_hours: int = 24
    ssl_verify: bool = False
    tg_proxy: Optional[str] = None

    @classmethod
    def from_env(cls) -> "AppConfig":
        c = cls()
        if v := os.getenv("CHECK_INTERVAL"):
            c.check_interval = int(v)
        if v := os.getenv("FRAGMENT_DURATION_MS"):
            c.fragment_duration_ms = int(v)
        if v := os.getenv("LOOKBACK_MINUTES"):
            c.lookback_minutes = int(v)
        if v := os.getenv("MAX_CONSECUTIVE_FAILS"):
            c.max_consecutive_fails = int(v)
        c.state_file = os.getenv("STATE_FILE", c.state_file)
        c.camera_id = os.getenv("CAMERA_ID", c.camera_id)
        c.ssl_verify = os.getenv("SSL_VERIFY", "false").lower() in ("true", "1", "yes")
        c.tg_proxy = os.getenv("TG_PROXY") or None
        return c


# ============================================================================
# Data models
# ============================================================================


@dataclass
class Recording:
    id: str
    camera_id: str
    start_time: int
    duration: int   # seconds, from Synology API
    size: int


@dataclass
class FragmentProgress:
    recording_id: str
    next_offset_ms: int = 0
    fragments_sent: int = 0
    last_attempt_time: float = 0
    consecutive_fails: int = 0
    is_completed: bool = False
    total_duration_ms: int = 0  # populated from recording.duration
    last_seen_time: float = 0


# ============================================================================
# Utils
# ============================================================================


def get_video_duration(file_path: str) -> Tuple[float, bool]:
    """Returns (duration_seconds, success) via ffprobe."""
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return 0.0, False

        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip()), True

        logger.debug(f"ffprobe error for {file_path}: {result.stderr.strip()}")

    except subprocess.TimeoutExpired:
        logger.warning(f"ffprobe timeout: {file_path}")
    except FileNotFoundError:
        logger.warning("ffprobe not found")
    except ValueError:
        logger.warning(f"ffprobe returned non-numeric output for {file_path}")
    except Exception as e:
        logger.error(f"get_video_duration unexpected error: {e}")

    return 0.0, False


# ============================================================================
# Synology API
# ============================================================================


class SynologyAPI:
    def __init__(self, config: AppConfig):
        syno_ip = os.getenv("SYNO_IP")
        syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{syno_ip}:{syno_port}/webapi/entry.cgi"
        self.ssl_verify = config.ssl_verify

        self.session = requests.Session()
        self.session.verify = self.ssl_verify
        if not self.ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.sid: Optional[str] = None
        self.last_login: Optional[float] = None
        self.cameras_cache: Dict[str, Dict] = {}
        self.api_version = "6"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(RequestException),
    )
    def login(self) -> bool:
        params = {
            "api": "SYNO.API.Auth",
            "version": "7",
            "method": "login",
            "account": os.getenv("SYNO_USER"),
            "passwd": os.getenv("SYNO_PASS"),
            "session": "SurveillanceStation",
            "format": "cookie",
        }
        if otp := os.getenv("SYNO_OTP"):
            params["otp_code"] = otp

        try:
            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                self.sid = data["data"]["sid"]
                self.last_login = time.time()
                logger.info("Аутентификация успешна")
                return True

            logger.error(f"Ошибка аутентификации: {data}")
            return False

        except RequestException as e:
            logger.error(f"Сетевая ошибка при аутентификации: {e}")
            raise

    def ensure_session(self) -> bool:
        if not self.sid or not self.last_login or time.time() - self.last_login > 600:
            return self.login()
        return True

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
    def get_cameras(self) -> Dict[str, Dict]:
        if not self.ensure_session():
            return {}

        try:
            response = self.session.get(
                self.base_url,
                params={
                    "api": "SYNO.SurveillanceStation.Camera",
                    "method": "List",
                    "version": "9",
                    "_sid": self.sid,
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                cameras = data.get("data", {}).get("cameras", [])
                self.cameras_cache = {
                    str(cam["id"]): {
                        "id": cam["id"],
                        "name": cam.get("newName", cam.get("name", f'Камера {cam["id"]}')),
                    }
                    for cam in cameras
                }
                logger.info(f"Загружено {len(cameras)} камер")
                return self.cameras_cache

            logger.warning(f"Не удалось получить камеры: {data}")
            return {}

        except RequestException as e:
            logger.error(f"Ошибка получения камер: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
    def get_recordings(
        self,
        camera_id: Optional[str] = None,
        limit: int = 30,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
    ) -> List[Recording]:
        if not self.ensure_session():
            return []

        try:
            current_time = int(time.time())
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "List",
                "version": self.api_version,
                "_sid": self.sid,
                "offset": "0",
                "limit": str(limit),
                "fromTime": str(from_time if from_time is not None else current_time - 300),
                "toTime": str(to_time if to_time is not None else current_time),
                "blIncludeThumb": "true",
            }
            if camera_id:
                params["cameraIds"] = str(camera_id)

            response = self.session.get(self.base_url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            if data.get("success"):
                recordings = []
                for rec in data.get("data", {}).get("recordings", []):
                    try:
                        start_time = rec.get("startTime", current_time - 60)
                        if start_time <= 0 or start_time > current_time:
                            start_time = current_time - 60
                        recordings.append(Recording(
                            id=str(rec["id"]),
                            camera_id=str(rec.get("cameraId", "unknown")),
                            start_time=start_time,
                            duration=rec.get("duration", 0),
                            size=rec.get("size", 0),
                        ))
                    except Exception as e:
                        logger.warning(f"Ошибка разбора записи {rec.get('id')}: {e}")

                logger.debug(f"Получено {len(recordings)} записей")
                return recordings

            error_code = data.get("error", {}).get("code", "unknown")
            logger.warning(f"Ошибка API (код {error_code})")
            return []

        except RequestException as e:
            logger.error(f"Ошибка получения записей: {e}")
            if "session" in str(e).lower():
                self.sid = None
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def download_fragment(
        self, recording_id: str, offset_ms: int, duration_ms: int
    ) -> Optional[str]:
        if not self.ensure_session():
            return None

        temp_path = None
        try:
            tf = tempfile.NamedTemporaryFile(
                suffix=f"_{recording_id}_{offset_ms}.mp4",
                delete=False,
                dir="/tmp",
            )
            tf.close()
            temp_path = tf.name

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
                f"Скачиваю фрагмент {recording_id}: "
                f"смещение={offset_ms/1000:.1f}с, длительность={duration_ms/1000:.1f}с"
            )

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=60
            )

            if response.status_code != 200:
                logger.debug(f"API вернул {response.status_code} для {recording_id}")
                os.remove(temp_path)
                return None

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(temp_path)
            if file_size > 0:
                logger.info(f"Фрагмент скачан: {file_size / (1024*1024):.1f} МБ")
                return temp_path

            logger.warning(f"Скачанный фрагмент пуст: {recording_id} offset={offset_ms}")
            os.remove(temp_path)
            return None

        except RequestException as e:
            logger.error(f"Ошибка скачивания {recording_id}: {e}")
            self._cleanup_temp(temp_path)
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при скачивании {recording_id}: {e}")
            self._cleanup_temp(temp_path)
            return None

    def _cleanup_temp(self, path: Optional[str]) -> None:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass

    def get_camera_name(self, camera_id: str) -> str:
        if not self.cameras_cache:
            self.get_cameras()
        cam = self.cameras_cache.get(str(camera_id))
        return cam["name"] if cam else f"Камера {camera_id}"


# ============================================================================
# Telegram Bot
# ============================================================================


class TelegramBot:
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB

    def __init__(self, proxy: Optional[str] = None):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.bot_name: Optional[str] = None
        self.session = requests.Session()
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
            logger.info(f"Telegram: используется прокси {proxy}")
        self._check_connection()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
    def _check_connection(self) -> None:
        response = self.session.get(f"{self.base_url}/getMe", timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("ok"):
            self.bot_name = data["result"]["first_name"]
            logger.info(f"Бот {self.bot_name} подключён к Telegram")
        else:
            raise RuntimeError(f"Telegram API error: {data}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=5),
        retry=retry_if_exception_type(RequestException),
    )
    def send_message(self, text: str) -> bool:
        try:
            response = self.session.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False

    def send_video(self, video_path: str, caption: str = "") -> bool:
        file_size = os.path.getsize(video_path)
        if file_size > self.MAX_FILE_SIZE:
            logger.warning(
                f"Файл слишком большой для Telegram: {file_size / (1024*1024):.1f} МБ"
            )
            return False

        logger.info(f"Отправляю видео в Telegram ({file_size / (1024*1024):.1f} МБ)")
        try:
            with open(video_path, "rb") as f:
                response = self.session.post(
                    f"{self.base_url}/sendVideo",
                    files={"video": f},
                    data={
                        "chat_id": self.chat_id,
                        "caption": caption,
                        "supports_streaming": True,
                        "parse_mode": "HTML",
                    },
                    timeout=120,
                )

            if response.status_code == 200 and response.json().get("ok"):
                logger.info("Видео успешно отправлено")
                return True

            logger.error(f"Telegram API ошибка: {response.status_code} — {response.text}")
            return False

        except Exception as e:
            logger.error(f"Ошибка отправки видео: {e}")
            return False


# ============================================================================
# State manager
# ============================================================================


class StateManager:
    def __init__(self, config: AppConfig):
        self.config = config
        self.state_file = Path(config.state_file)
        self.progress: Dict[str, FragmentProgress] = {}
        self.completed_ids: Set[str] = set()
        self._load()

    def _load(self) -> None:
        try:
            if not self.state_file.exists():
                return
            with open(self.state_file) as f:
                state = json.load(f)

            self.completed_ids = set(state.get("completed_ids", []))
            for rec_id, d in state.get("progress", {}).items():
                self.progress[rec_id] = FragmentProgress(
                    recording_id=rec_id,
                    next_offset_ms=d.get("next_offset_ms", 0),
                    fragments_sent=d.get("fragments_sent", 0),
                    last_attempt_time=d.get("last_attempt_time", 0),
                    consecutive_fails=d.get("consecutive_fails", 0),
                    is_completed=d.get("is_completed", False),
                    total_duration_ms=d.get("total_duration_ms", 0),
                    last_seen_time=d.get("last_seen_time", 0),
                )
            logger.info(f"Состояние загружено: {len(self.progress)} активных записей")
        except Exception as e:
            logger.warning(f"Не удалось загрузить состояние: {e}")

    def save(self) -> None:
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            state = {
                "completed_ids": list(self.completed_ids),
                "progress": {
                    rec_id: {
                        "next_offset_ms": p.next_offset_ms,
                        "fragments_sent": p.fragments_sent,
                        "last_attempt_time": p.last_attempt_time,
                        "consecutive_fails": p.consecutive_fails,
                        "is_completed": p.is_completed,
                        "total_duration_ms": p.total_duration_ms,
                        "last_seen_time": p.last_seen_time,
                    }
                    for rec_id, p in self.progress.items()
                },
                "updated_at": datetime.now().isoformat(),
            }
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния: {e}")

    def is_completed(self, recording_id: str) -> bool:
        return recording_id in self.completed_ids or (
            recording_id in self.progress and self.progress[recording_id].is_completed
        )

    def get_or_create_progress(self, recording: Recording) -> FragmentProgress:
        rec_id = recording.id
        if rec_id not in self.progress:
            self.progress[rec_id] = FragmentProgress(
                recording_id=rec_id,
                last_seen_time=time.time(),
            )
            logger.info(f"Новая запись: {rec_id}")

        p = self.progress[rec_id]
        p.last_seen_time = time.time()

        # Update total duration from API whenever it becomes available
        if recording.duration > 0:
            p.total_duration_ms = recording.duration * 1000

        return p

    def mark_fragment_sent(self, recording_id: str, next_offset_ms: int) -> None:
        if recording_id in self.progress:
            p = self.progress[recording_id]
            p.next_offset_ms = next_offset_ms
            p.fragments_sent += 1
            p.last_attempt_time = time.time()
            p.consecutive_fails = 0
        self.save()

    def mark_fragment_failed(self, recording_id: str) -> None:
        if recording_id in self.progress:
            p = self.progress[recording_id]
            p.last_attempt_time = time.time()
            p.consecutive_fails += 1
        self.save()

    def mark_completed(self, recording_id: str) -> None:
        if recording_id in self.progress:
            self.progress[recording_id].is_completed = True
        self.completed_ids.add(recording_id)
        self.save()
        logger.info(f"Запись {recording_id} помечена как завершённая")

    def get_active_ids(self) -> List[str]:
        return [r for r, p in self.progress.items() if not p.is_completed]

    def cleanup_old(self) -> None:
        cutoff = time.time() - self.config.cleanup_max_age_hours * 3600
        old = [r for r, p in self.progress.items() if p.last_seen_time < cutoff]
        for r in old:
            del self.progress[r]
        if old:
            logger.info(f"Очищено {len(old)} старых записей")
        self.save()

    def stats(self) -> Dict[str, int]:
        return {
            "active": len(self.get_active_ids()),
            "completed": len(self.completed_ids),
            "fragments": sum(p.fragments_sent for p in self.progress.values()),
        }


# ============================================================================
# Fragment processing
# ============================================================================


def _format_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    offset_s: float,
    duration_s: float,
) -> str:
    start_dt = datetime.fromtimestamp(recording.start_time + offset_s)
    return (
        f"<b>🚨 Обнаружено движение (фрагмент {fragment_num})</b>\n\n"
        f"<b>📅 Дата:</b> {start_dt.strftime('%d.%m.%Y')}\n"
        f"<b>🕐 Время:</b> {start_dt.strftime('%H:%M:%S')}\n"
        f"<b>📷 Камера:</b> {camera_name}\n"
        f"<b>⏱️ Позиция:</b> {offset_s:.1f}–{offset_s + duration_s:.1f} сек\n"
        f"<b>🎬 Длительность:</b> {duration_s:.1f} сек"
    )


def process_recording(
    synology: SynologyAPI,
    telegram: TelegramBot,
    state: StateManager,
    recording: Recording,
    camera_name: str,
    fragment_duration_ms: int,
    max_consecutive_fails: int,
) -> int:
    """
    Downloads and sends all currently available fragments for a recording.
    Returns the number of fragments successfully sent this call.
    Stops when no more data is available (empty download) or an error occurs.
    """
    sent = 0
    progress = state.get_or_create_progress(recording)

    while not progress.is_completed:
        # If we know the total duration, check if we're done
        if progress.total_duration_ms > 0 and progress.next_offset_ms >= progress.total_duration_ms:
            state.mark_completed(recording.id)
            break

        # Trim last fragment to remaining duration if known
        if progress.total_duration_ms > 0:
            remaining_ms = progress.total_duration_ms - progress.next_offset_ms
            request_duration_ms = min(fragment_duration_ms, remaining_ms)
        else:
            request_duration_ms = fragment_duration_ms

        fragment_file = synology.download_fragment(
            recording.id, progress.next_offset_ms, request_duration_ms
        )

        if not fragment_file:
            state.mark_fragment_failed(recording.id)
            if progress.consecutive_fails >= max_consecutive_fails:
                logger.info(
                    f"Запись {recording.id}: {max_consecutive_fails} неудачных попыток подряд, "
                    f"помечаю как завершённую"
                )
                state.mark_completed(recording.id)
            # No more data available right now; stop trying this recording this cycle
            break

        try:
            actual_duration, ok = get_video_duration(fragment_file)
            if not ok or actual_duration <= 0:
                actual_duration = request_duration_ms / 1000

            caption = _format_caption(
                recording,
                camera_name,
                progress.fragments_sent + 1,
                progress.next_offset_ms / 1000,
                actual_duration,
            )

            if telegram.send_video(fragment_file, caption):
                next_offset = progress.next_offset_ms + int(actual_duration * 1000)
                state.mark_fragment_sent(recording.id, next_offset)
                sent += 1

                if progress.total_duration_ms > 0 and progress.next_offset_ms >= progress.total_duration_ms:
                    state.mark_completed(recording.id)
            else:
                state.mark_fragment_failed(recording.id)
                break

        finally:
            try:
                os.remove(fragment_file)
            except OSError:
                pass

    return sent


# ============================================================================
# Main
# ============================================================================


def main() -> None:
    logger.info("Запуск Surveillance Station Telegram Bot")

    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Отсутствуют обязательные переменные окружения: {missing}")
        return

    config = AppConfig.from_env()
    synology = SynologyAPI(config)
    telegram = TelegramBot(proxy=config.tg_proxy)
    state = StateManager(config)

    synology.get_cameras()
    camera_name = synology.get_camera_name(config.camera_id)

    s = state.stats()
    telegram.send_message(
        f"<b>🟢 Бот запущен</b>\n\n"
        f"<b>🤖 Бот:</b> {telegram.bot_name}\n"
        f"<b>📷 Камера:</b> {camera_name} (ID: {config.camera_id})\n"
        f"<b>🔄 Интервал проверки:</b> {config.check_interval} сек\n"
        f"<b>⏱️ Длительность фрагмента:</b> {config.fragment_duration_ms / 1000} сек\n"
        f"<b>🕐 Глубина поиска:</b> {config.lookback_minutes} мин\n"
        f"<b>📊 Активных записей:</b> {s['active']}\n"
        f"<b>📈 Завершённых записей:</b> {s['completed']}"
    )

    logger.info(f"Камера: {camera_name} (ID: {config.camera_id})")
    logger.info(
        f"Интервал: {config.check_interval}с, "
        f"фрагмент: {config.fragment_duration_ms/1000}с, "
        f"глубина: {config.lookback_minutes} мин"
    )

    shutdown_requested = False
    fragments_this_session = 0
    last_cleanup = time.time()
    start_time = time.time()
    healthcheck = Path("/tmp/healthcheck")

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Получен сигнал {signum}, завершаю работу...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Начинаю мониторинг...")

    while not shutdown_requested:
        try:
            current_time = int(time.time())
            from_time = current_time - config.lookback_minutes * 60

            recordings = synology.get_recordings(
                camera_id=config.camera_id,
                limit=30,
                from_time=from_time,
                to_time=current_time,
            )

            seen_ids = {r.id for r in recordings}

            for recording in recordings:
                if shutdown_requested:
                    break
                if state.is_completed(recording.id):
                    continue
                fragments_this_session += process_recording(
                    synology, telegram, state, recording,
                    camera_name, config.fragment_duration_ms, config.max_consecutive_fails,
                )

            # Mark recordings that have disappeared from the API window as completed
            for rec_id in state.get_active_ids():
                if rec_id not in seen_ids:
                    p = state.progress.get(rec_id)
                    if p and time.time() - p.last_seen_time > 60:
                        logger.info(f"Запись {rec_id} пропала из API, помечаю как завершённую")
                        state.mark_completed(rec_id)

            # Periodic cleanup and stats
            if time.time() - last_cleanup > 300:
                state.cleanup_old()
                s = state.stats()
                logger.info(
                    f"Статистика: {s['active']} активных, "
                    f"{s['completed']} завершённых, "
                    f"{s['fragments']} фрагментов"
                )
                last_cleanup = time.time()

            healthcheck.touch()

            for _ in range(config.check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            shutdown_requested = True
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
            time.sleep(10)

    session_duration = time.time() - start_time
    s = state.stats()

    telegram.send_message(
        f"<b>🔴 Бот остановлен</b>\n\n"
        f"<b>🤖 Бот:</b> {telegram.bot_name}\n"
        f"<b>⏱️ Время работы:</b> {session_duration:.0f} сек\n"
        f"<b>📊 Отправлено фрагментов:</b> {fragments_this_session}\n"
        f"<b>📈 Завершённых записей:</b> {s['completed']}"
    )

    state.save()
    logger.info(f"Завершение. Время: {session_duration:.0f}с, фрагментов: {fragments_this_session}")


if __name__ == "__main__":
    main()
