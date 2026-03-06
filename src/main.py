#!/usr/bin/env python3
"""Surveillance Station -> Telegram Bot (rewrite v2)"""

import os
import json
import time
import signal
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple
from dataclasses import dataclass, field
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
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
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
    lookback_minutes: int = 30
    max_consecutive_fails: int = 3
    max_fragments_per_cycle: int = 20
    cleanup_max_age_hours: int = 24
    ssl_verify: bool = False
    tg_proxy: Optional[str] = None
    # How many cycles at the end of a recording before marking it complete
    end_stable_cycles: int = 2

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
        if v := os.getenv("MAX_FRAGMENTS_PER_CYCLE"):
            c.max_fragments_per_cycle = int(v)
        if v := os.getenv("END_STABLE_CYCLES"):
            c.end_stable_cycles = int(v)
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
    start_time: int   # unix timestamp
    duration: int     # seconds, from Synology API
    size: int


@dataclass
class RecordingProgress:
    recording_id: str
    next_offset_ms: int = 0          # next offset to download
    fragments_sent: int = 0
    consecutive_fails: int = 0
    is_completed: bool = False
    known_duration_ms: int = 0       # latest known duration (from API, in ms)
    # How many consecutive cycles we've been at the end with stable duration
    cycles_at_end: int = 0
    last_seen_time: float = field(default_factory=time.time)


# ============================================================================
# Utils
# ============================================================================


def get_video_duration(file_path: str) -> Tuple[float, bool]:
    """Returns (duration_seconds, success) via ffprobe."""
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logger.debug(f"ffprobe: file empty or missing: {file_path}")
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
            timeout=10,
        )

        if result.returncode == 0 and result.stdout.strip():
            dur = float(result.stdout.strip())
            logger.debug(f"ffprobe: {file_path} -> {dur:.3f}s")
            return dur, True

        logger.debug(f"ffprobe error (rc={result.returncode}): {result.stderr.strip()[:200]}")

    except subprocess.TimeoutExpired:
        logger.warning("ffprobe timeout")
    except FileNotFoundError:
        logger.warning("ffprobe not found — install ffmpeg")
    except ValueError as e:
        logger.warning(f"ffprobe non-numeric output: {e}")
    except Exception as e:
        logger.error(f"ffprobe unexpected error: {e}")

    return 0.0, False


# ============================================================================
# Synology API
# ============================================================================


class SynologyAPI:
    # Tested recording API version
    RECORDING_API_VERSION = "6"

    def __init__(self, config: AppConfig):
        syno_ip = os.getenv("SYNO_IP")
        syno_port = os.getenv("SYNO_PORT", "5001")
        self.base_url = f"https://{syno_ip}:{syno_port}/webapi/entry.cgi"
        self.ssl_verify = config.ssl_verify
        self.config = config

        self.session = requests.Session()
        self.session.verify = self.ssl_verify
        if not self.ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.sid: Optional[str] = None
        self.last_login: Optional[float] = None
        self.cameras_cache: Dict[str, Dict] = {}

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

        response = self.session.get(self.base_url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("success"):
            self.sid = data["data"]["sid"]
            self.last_login = time.time()
            logger.info("Synology: auth OK")
            return True

        logger.error(f"Synology auth failed: {data}")
        return False

    def ensure_session(self) -> bool:
        if not self.sid or not self.last_login or time.time() - self.last_login > 600:
            return self.login()
        return True

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
    def get_cameras(self) -> Dict[str, Dict]:
        if not self.ensure_session():
            return {}

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
                    "name": cam.get("newName", cam.get("name", f"Camera {cam['id']}")),
                }
                for cam in cameras
            }
            logger.info(f"Cameras loaded: {[c['name'] for c in self.cameras_cache.values()]}")
            return self.cameras_cache

        logger.warning(f"get_cameras failed: {data}")
        return {}

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

        current_time = int(time.time())
        params = {
            "api": "SYNO.SurveillanceStation.Recording",
            "method": "List",
            "version": self.RECORDING_API_VERSION,
            "_sid": self.sid,
            "offset": "0",
            "limit": str(limit),
            "fromTime": str(from_time if from_time is not None else current_time - 300),
            "toTime": str(to_time if to_time is not None else current_time),
            "blIncludeThumb": "false",
        }
        if camera_id:
            params["cameraIds"] = str(camera_id)

        response = self.session.get(self.base_url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"get_recordings raw: {json.dumps(data)[:500]}")

        if data.get("success"):
            recordings = []
            for rec in data.get("data", {}).get("recordings", []):
                try:
                    # API v6 often omits startTime and duration.
                    # Extract unix timestamp from filePath when available.
                    # Format: .../camera-YYYYMMDD-HHMMSS-<unix_ms>-N.mp4
                    start_time = rec.get("startTime") or 0
                    if not start_time:
                        fp = rec.get("filePath", "")
                        for part in fp.replace(".mp4", "").split("-"):
                            if len(part) == 13 and part.isdigit():
                                start_time = int(part) // 1000  # ms -> s
                                break
                    if not start_time or start_time > current_time:
                        start_time = current_time - 60

                    duration = int(rec.get("duration") or 0)

                    recordings.append(Recording(
                        id=str(rec["id"]),
                        camera_id=str(rec.get("cameraId", "unknown")),
                        start_time=start_time,
                        duration=duration,
                        size=int(rec.get("sizeByte") or rec.get("size") or 0),
                    ))
                    logger.debug(
                        f"  rec id={rec['id']} duration={duration}s "
                        f"start={start_time} path={rec.get('filePath', '')}"
                    )
                except Exception as e:
                    logger.warning(f"Recording parse error {rec.get('id')}: {e}")

            logger.info(f"get_recordings: {len(recordings)} recordings found")
            return recordings

        error_code = data.get("error", {}).get("code", "unknown")
        logger.warning(f"get_recordings API error code={error_code}")
        return []

    def download_fragment(
        self, recording_id: str, offset_ms: int, duration_ms: int
    ) -> Optional[str]:
        """Download a fragment. Returns temp file path or None on failure."""
        if not self.ensure_session():
            return None

        temp_path = None
        try:
            tf = tempfile.NamedTemporaryFile(
                suffix=f"_rec{recording_id}_off{offset_ms}.mp4",
                delete=False,
                dir="/tmp",
            )
            tf.close()
            temp_path = tf.name

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": self.RECORDING_API_VERSION,
                "_sid": self.sid,
                "id": recording_id,
                "mountId": "0",
                "offsetTimeMs": str(offset_ms),
                "playTimeMs": str(duration_ms),
            }

            logger.debug(
                f"download_fragment: rec={recording_id} offset={offset_ms}ms "
                f"duration={duration_ms}ms"
            )

            response = self.session.get(
                self.base_url, params=params, stream=True, timeout=90
            )

            if response.status_code != 200:
                logger.warning(
                    f"download_fragment: HTTP {response.status_code} for rec={recording_id}"
                )
                os.remove(temp_path)
                return None

            content_type = response.headers.get("Content-Type", "")
            logger.debug(f"download_fragment: Content-Type={content_type}")

            # If Synology returns a JSON error instead of video data, bail out
            if "application/json" in content_type:
                body = response.text[:300]
                logger.warning(f"download_fragment: got JSON instead of video: {body}")
                os.remove(temp_path)
                return None

            try:
                with open(temp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=32768):
                        if chunk:
                            f.write(chunk)
            except RequestException as e:
                # IncompleteRead / ChunkedEncodingError — partial download.
                # If we got enough data (> 100 KB), the MP4 may still be valid.
                file_size = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
                if file_size > 102400:
                    logger.warning(
                        f"download_fragment: partial download {file_size/1024:.0f} KB "
                        f"(IncompleteRead) for rec={recording_id} offset={offset_ms}ms "
                        f"— trying to use partial file"
                    )
                    return temp_path
                logger.error(
                    f"download_fragment: IncompleteRead too small ({file_size} B) "
                    f"rec={recording_id}: {e}"
                )
                self._cleanup_temp(temp_path)
                return None

            file_size = os.path.getsize(temp_path)
            logger.debug(
                f"download_fragment: rec={recording_id} offset={offset_ms}ms "
                f"-> {file_size/1024:.1f} KB"
            )

            if file_size > 0:
                return temp_path

            logger.info(
                f"download_fragment: empty file for rec={recording_id} offset={offset_ms}ms "
                f"(live edge or end of recording)"
            )
            os.remove(temp_path)
            return None

        except RequestException as e:
            logger.error(f"download_fragment network error rec={recording_id}: {e}")
            if "401" in str(e) or "session" in str(e).lower():
                self.sid = None
            self._cleanup_temp(temp_path)
            return None
        except Exception as e:
            logger.error(f"download_fragment unexpected error rec={recording_id}: {e}")
            self._cleanup_temp(temp_path)
            return None

    def _cleanup_temp(self, path: Optional[str]) -> None:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass

    def get_camera_name(self, camera_id: str) -> str:
        if not self.cameras_cache:
            self.get_cameras()
        cam = self.cameras_cache.get(str(camera_id))
        return cam["name"] if cam else f"Camera {camera_id}"


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
            logger.info(f"Telegram: using proxy {proxy}")
        self._check_connection()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
    def _check_connection(self) -> None:
        response = self.session.get(f"{self.base_url}/getMe", timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("ok"):
            self.bot_name = data["result"]["first_name"]
            logger.info(f"Telegram: connected as '{self.bot_name}'")
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
            logger.error(f"send_message error: {e}")
            return False

    def send_video(self, video_path: str, caption: str = "") -> bool:
        file_size = os.path.getsize(video_path)
        if file_size > self.MAX_FILE_SIZE:
            logger.warning(f"File too large for Telegram: {file_size/1024/1024:.1f} MB")
            return False

        logger.info(f"Sending video {file_size/1024/1024:.2f} MB to Telegram")

        for attempt in range(3):
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
                        timeout=180,
                    )

                if response.status_code == 200 and response.json().get("ok"):
                    logger.info("Video sent OK")
                    return True

                if response.status_code == 429:
                    retry_after = response.json().get("parameters", {}).get("retry_after", 30)
                    logger.warning(f"Telegram 429: waiting {retry_after}s (attempt {attempt+1}/3)")
                    time.sleep(retry_after)
                    continue

                logger.error(
                    f"Telegram sendVideo error: HTTP {response.status_code} "
                    f"body={response.text[:200]}"
                )
                return False

            except Exception as e:
                logger.error(f"send_video exception (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(5)

        return False


# ============================================================================
# State manager
# ============================================================================


class StateManager:
    def __init__(self, config: AppConfig):
        self.state_file = Path(config.state_file)
        self.progress: Dict[str, RecordingProgress] = {}
        self.completed_ids: Set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self.state_file.exists():
            logger.info("No state file, starting fresh")
            return
        try:
            with open(self.state_file) as f:
                state = json.load(f)

            self.completed_ids = set(state.get("completed_ids", []))
            for rec_id, d in state.get("progress", {}).items():
                self.progress[rec_id] = RecordingProgress(
                    recording_id=rec_id,
                    next_offset_ms=d.get("next_offset_ms", 0),
                    fragments_sent=d.get("fragments_sent", 0),
                    consecutive_fails=d.get("consecutive_fails", 0),
                    is_completed=d.get("is_completed", False),
                    known_duration_ms=d.get("known_duration_ms", 0),
                    cycles_at_end=d.get("cycles_at_end", 0),
                    last_seen_time=d.get("last_seen_time", time.time()),
                )
            logger.info(
                f"State loaded: {len(self.progress)} progress entries, "
                f"{len(self.completed_ids)} completed"
            )
        except Exception as e:
            logger.warning(f"State load error (starting fresh): {e}")
            self.progress = {}
            self.completed_ids = set()

    def save(self) -> None:
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            state = {
                "completed_ids": list(self.completed_ids),
                "progress": {
                    rec_id: {
                        "next_offset_ms": p.next_offset_ms,
                        "fragments_sent": p.fragments_sent,
                        "consecutive_fails": p.consecutive_fails,
                        "is_completed": p.is_completed,
                        "known_duration_ms": p.known_duration_ms,
                        "cycles_at_end": p.cycles_at_end,
                        "last_seen_time": p.last_seen_time,
                    }
                    for rec_id, p in self.progress.items()
                },
                "updated_at": datetime.now().isoformat(),
            }
            tmp = self.state_file.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            tmp.replace(self.state_file)
        except Exception as e:
            logger.error(f"State save error: {e}")

    def is_completed(self, recording_id: str) -> bool:
        return recording_id in self.completed_ids or (
            recording_id in self.progress and self.progress[recording_id].is_completed
        )

    def get_or_create(self, recording: Recording) -> RecordingProgress:
        rec_id = recording.id
        if rec_id not in self.progress:
            logger.info(f"New recording: id={rec_id} duration={recording.duration}s")
            self.progress[rec_id] = RecordingProgress(recording_id=rec_id)

        p = self.progress[rec_id]
        p.last_seen_time = time.time()
        return p

    def mark_sent(self, recording_id: str, new_offset_ms: int) -> None:
        if recording_id in self.progress:
            p = self.progress[recording_id]
            old_offset = p.next_offset_ms
            p.next_offset_ms = new_offset_ms
            p.fragments_sent += 1
            p.consecutive_fails = 0
            p.cycles_at_end = 0
            logger.debug(
                f"mark_sent: rec={recording_id} offset {old_offset}->{new_offset_ms}ms "
                f"(total sent: {p.fragments_sent})"
            )
        self.save()

    def mark_failed(self, recording_id: str) -> None:
        if recording_id in self.progress:
            self.progress[recording_id].consecutive_fails += 1
        self.save()

    def mark_completed(self, recording_id: str, reason: str = "") -> None:
        if recording_id in self.progress:
            self.progress[recording_id].is_completed = True
        self.completed_ids.add(recording_id)
        self.save()
        logger.info(f"Recording {recording_id} completed. Reason: {reason}")

    def get_active_ids(self) -> List[str]:
        return [r for r, p in self.progress.items() if not p.is_completed]

    def cleanup_old(self, max_age_hours: int = 24) -> None:
        cutoff = time.time() - max_age_hours * 3600
        old = [r for r, p in self.progress.items() if p.last_seen_time < cutoff]
        for r in old:
            del self.progress[r]
        if old:
            logger.info(f"Cleaned up {len(old)} old recording entries")
        self.save()

    def stats(self) -> Dict[str, int]:
        active = self.get_active_ids()
        return {
            "active": len(active),
            "completed": len(self.completed_ids),
            "fragments_total": sum(p.fragments_sent for p in self.progress.values()),
        }


# ============================================================================
# Fragment processing
# ============================================================================

MIN_VALID_FRAGMENT_S = 0.5   # Shorter than this = no data yet at live edge


def _format_caption(
    recording: Recording,
    camera_name: str,
    fragment_num: int,
    offset_s: float,
    duration_s: float,
) -> str:
    ts = datetime.fromtimestamp(recording.start_time + offset_s)
    return (
        f"<b>Motion detected (fragment {fragment_num})</b>\n"
        f"<b>Date:</b> {ts.strftime('%d.%m.%Y')}\n"
        f"<b>Time:</b> {ts.strftime('%H:%M:%S')}\n"
        f"<b>Camera:</b> {camera_name}\n"
        f"<b>Position:</b> {offset_s:.0f}s - {offset_s + duration_s:.0f}s"
    )


def process_recording(
    synology: SynologyAPI,
    telegram: TelegramBot,
    state: StateManager,
    recording: Recording,
    camera_name: str,
    config: AppConfig,
) -> int:
    """
    Process one recording: download and send all currently available fragments.
    Returns number of fragments sent this call.

    Completion logic:
    - If next_offset_ms >= known_duration_ms AND known_duration_ms has not changed
      for `end_stable_cycles` consecutive cycles -> mark completed.
    - This handles live-appending recordings (duration grows while recording).
    """
    if state.is_completed(recording.id):
        return 0

    progress = state.get_or_create(recording)

    # Update known duration from the latest API response
    api_duration_ms = recording.duration * 1000
    if api_duration_ms > progress.known_duration_ms:
        logger.debug(
            f"rec={recording.id}: duration grew "
            f"{progress.known_duration_ms}->{api_duration_ms}ms, resetting end counter"
        )
        progress.known_duration_ms = api_duration_ms
        progress.cycles_at_end = 0  # duration grew -> not stable yet

    # Check stable-end completion
    if (
        progress.known_duration_ms > 0
        and progress.next_offset_ms >= progress.known_duration_ms
        and progress.cycles_at_end >= config.end_stable_cycles
    ):
        state.mark_completed(
            recording.id,
            reason=f"reached end at {progress.known_duration_ms}ms for "
                   f"{progress.cycles_at_end} cycles",
        )
        return 0

    sent = 0
    fragments_this_call = 0
    last_fragment_size = -1  # used to detect end-of-recording API loop

    while not progress.is_completed:
        # Per-call fragment limit
        if fragments_this_call >= config.max_fragments_per_cycle:
            logger.debug(
                f"rec={recording.id}: hit max_fragments_per_cycle={config.max_fragments_per_cycle}, "
                f"continuing next cycle"
            )
            break

        # Check if we've reached the end of available data
        if progress.known_duration_ms > 0 and progress.next_offset_ms >= progress.known_duration_ms:
            progress.cycles_at_end += 1
            state.save()
            logger.info(
                f"rec={recording.id}: at end "
                f"(offset={progress.next_offset_ms}ms >= duration={progress.known_duration_ms}ms, "
                f"cycles_at_end={progress.cycles_at_end}/{config.end_stable_cycles})"
            )
            break

        # Calculate request window
        if progress.known_duration_ms > 0:
            remaining_ms = progress.known_duration_ms - progress.next_offset_ms
            request_ms = min(config.fragment_duration_ms, remaining_ms)
        else:
            # Duration unknown (ongoing recording) - use full fragment size
            request_ms = config.fragment_duration_ms

        logger.info(
            f"rec={recording.id}: downloading fragment "
            f"offset={progress.next_offset_ms}ms request={request_ms}ms "
            f"(known_duration={progress.known_duration_ms}ms)"
        )

        fragment_file = synology.download_fragment(recording.id, progress.next_offset_ms, request_ms)

        if not fragment_file:
            progress.consecutive_fails += 1
            state.save()
            logger.warning(
                f"rec={recording.id}: download failed "
                f"(consecutive_fails={progress.consecutive_fails})"
            )
            if progress.consecutive_fails >= config.max_consecutive_fails:
                logger.warning(
                    f"rec={recording.id}: too many consecutive failures, skipping this cycle"
                )
            break

        try:
            actual_duration, ok = get_video_duration(fragment_file)
            file_size = os.path.getsize(fragment_file)

            logger.info(
                f"rec={recording.id}: fragment downloaded "
                f"size={file_size/1024:.1f}KB actual_duration={actual_duration:.3f}s "
                f"ffprobe_ok={ok}"
            )

            if not ok or actual_duration < MIN_VALID_FRAGMENT_S:
                # Live edge: no real content yet in this time slot
                logger.info(
                    f"rec={recording.id}: fragment too short ({actual_duration:.3f}s < "
                    f"{MIN_VALID_FRAGMENT_S}s) — at live edge, waiting next cycle"
                )
                progress.cycles_at_end += 1
                state.save()
                break

            # Valid fragment — reset failure counter
            progress.consecutive_fails = 0

            caption = _format_caption(
                recording,
                camera_name,
                progress.fragments_sent + 1,
                progress.next_offset_ms / 1000,
                actual_duration,
            )

            if telegram.send_video(fragment_file, caption):
                # Advance by actual measured duration (from ffprobe)
                new_offset = progress.next_offset_ms + int(actual_duration * 1000)
                state.mark_sent(recording.id, new_offset)
                sent += 1
                fragments_this_call += 1

                # Detect end-of-recording API loop:
                # When offset > actual recording length, Synology returns the same
                # full-recording file regardless of offset. Two identical file sizes
                # at different offsets indicate a loop — BUT only when the returned
                # fragment is significantly longer than requested (i.e. the API gave
                # us the whole recording instead of just the requested window).
                oversized = actual_duration > (request_ms / 1000) * 1.5
                if file_size == last_fragment_size and last_fragment_size > 0 and oversized:
                    logger.info(
                        f"rec={recording.id}: duplicate file size {file_size/1024:.0f} KB "
                        f"(actual {actual_duration:.1f}s >> requested {request_ms/1000:.0f}s) "
                        f"at offset {progress.next_offset_ms}ms — past end of recording, completing"
                    )
                    state.mark_completed(recording.id, reason="repeated oversized file (API loop detected)")
                    break
                last_fragment_size = file_size
            else:
                state.mark_failed(recording.id)
                logger.warning(f"rec={recording.id}: send_video failed, stopping this cycle")
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
    logger.info("Starting Surveillance Station Telegram Bot v2")

    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required env vars: {missing}")
        return

    config = AppConfig.from_env()

    logger.info(
        f"Config: check_interval={config.check_interval}s "
        f"fragment={config.fragment_duration_ms/1000}s "
        f"lookback={config.lookback_minutes}min "
        f"camera_id={config.camera_id} "
        f"max_frags_per_cycle={config.max_fragments_per_cycle}"
    )

    synology = SynologyAPI(config)
    telegram = TelegramBot(proxy=config.tg_proxy)
    state = StateManager(config)

    try:
        synology.get_cameras()
    except Exception as e:
        logger.warning(f"Could not fetch camera list (non-fatal): {e}")
    camera_name = synology.get_camera_name(config.camera_id)

    s = state.stats()
    telegram.send_message(
        f"<b>Bot started</b>\n"
        f"Camera: {camera_name} (ID: {config.camera_id})\n"
        f"Check interval: {config.check_interval}s\n"
        f"Fragment duration: {config.fragment_duration_ms/1000}s\n"
        f"Lookback: {config.lookback_minutes}min\n"
        f"Active recordings in state: {s['active']}"
    )

    shutdown_requested = False
    fragments_this_session = 0
    last_cleanup = time.time()
    start_time = time.time()
    healthcheck = Path("/tmp/healthcheck")
    cycle = 0

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Signal {signum} received, shutting down...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Monitoring started")

    while not shutdown_requested:
        try:
            cycle += 1
            current_time = int(time.time())
            from_time = current_time - config.lookback_minutes * 60

            logger.debug(f"--- Cycle {cycle} ---")

            recordings = synology.get_recordings(
                camera_id=config.camera_id,
                limit=50,
                from_time=from_time,
                to_time=current_time,
            )

            seen_ids = {r.id for r in recordings}

            for recording in recordings:
                if shutdown_requested:
                    break
                if state.is_completed(recording.id):
                    logger.debug(f"rec={recording.id}: already completed, skipping")
                    continue
                n = process_recording(synology, telegram, state, recording, camera_name, config)
                fragments_this_session += n

            # Mark recordings that disappeared from the API as completed
            for rec_id in state.get_active_ids():
                if rec_id not in seen_ids:
                    p = state.progress.get(rec_id)
                    if p and time.time() - p.last_seen_time > 120:
                        state.mark_completed(
                            rec_id,
                            reason="disappeared from API for >120s",
                        )

            # Periodic cleanup and stats
            if time.time() - last_cleanup > 300:
                state.cleanup_old(config.cleanup_max_age_hours)
                s = state.stats()
                logger.info(
                    f"Stats: active={s['active']} completed={s['completed']} "
                    f"fragments_total={s['fragments_total']} "
                    f"session_sent={fragments_this_session}"
                )
                last_cleanup = time.time()

            healthcheck.touch()

            # Sleep in small increments to allow fast signal handling
            for _ in range(config.check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            shutdown_requested = True
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(10)

    session_duration = time.time() - start_time
    s = state.stats()
    telegram.send_message(
        f"<b>Bot stopped</b>\n"
        f"Uptime: {session_duration:.0f}s\n"
        f"Fragments sent: {fragments_this_session}\n"
        f"Completed recordings: {s['completed']}"
    )
    state.save()
    logger.info(
        f"Shutdown. Uptime: {session_duration:.0f}s fragments_sent: {fragments_this_session}"
    )


if __name__ == "__main__":
    main()
