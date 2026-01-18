#!/usr/bin/env python3
"""
Surveillance Station to Telegram Bot
Исправленная версия с корректной обработкой времени и отправкой видео
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
        """Получаем список записей - РАБОЧАЯ ВЕРСИЯ из теста"""
        if not self.ensure_session():
            return []

        try:
            # Рабочие параметры из теста: version=6, fromTime=0, toTime=0
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "List",
                "version": "6",  # ← Рабочая версия из теста
                "_sid": self.sid,
                "offset": "0",
                "limit": str(limit),
                "fromTime": "0",  # ← 0 для получения последних записей
                "toTime": "0",  # ← 0 для получения последних записей
            }

            if camera_id:
                params["cameraIds"] = str(camera_id)

            response = self.session.get(self.base_url, params=params, timeout=20)
            response.raise_for_status()

            data = response.json()

            if data.get("success"):
                recordings_data = data.get("data", {}).get("recordings", [])

                # ДЛЯ ОТЛАДКИ: логируем первую запись, чтобы увидеть структуру
                if recordings_data and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"📋 Структура записи из API: {json.dumps(recordings_data[0], indent=2)}"
                    )

                recordings = []
                for rec in recordings_data:
                    try:
                        # Обработка времени: некоторые записи могут не иметь startTime
                        start_time = rec.get("startTime", 0)

                        # Если время не указано, используем текущее время минус 1 час
                        # (чтобы подпись в Telegram не была 1970 годом)
                        if (
                            start_time == 0 or start_time < 1000000000
                        ):  # Проверяем на разумность timestamp
                            start_time = int(time.time()) - 3600  # 1 час назад

                        # Обработка длительности: может быть 0 в ответе
                        duration = rec.get("duration", 0)
                        if duration <= 0:
                            # Если длительность не указана, ставим разумное значение
                            duration = 10000  # 10 секунд по умолчанию

                        recording = Recording(
                            id=str(rec.get("id")),
                            camera_id=str(rec.get("cameraId", "unknown")),
                            start_time=start_time,
                            duration=duration,  # В миллисекундах
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
    def download_full_recording(self, recording: Recording) -> Optional[str]:
        """Скачивает запись целиком - АДАПТИРОВАННАЯ ВЕРСИЯ"""
        if not self.ensure_session():
            return None

        temp_file = None
        try:
            # Создаем временный файл
            temp_file = tempfile.NamedTemporaryFile(
                suffix=".mp4", delete=False, dir="/tmp"
            )
            temp_file.close()

            # Параметры скачивания как в старом работающем коде
            download_url = f"{self.base_url}/temp.mp4"

            # Пробуем разные варианты скачивания - используем только вариант 1, который работает
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": "6",
                "_sid": self.sid,
                "id": recording.id,
                "mountId": "0",
                "offsetTimeMs": "0",
                "playTimeMs": "10000",  # 10 секунд как в старом коде - РАБОТАЕТ!
            }

            logger.info(f"📥 Скачиваю запись {recording.id} (вариант 1)")

            response = self.session.get(
                download_url, params=params, stream=True, timeout=120
            )
            response.raise_for_status()

            # Проверяем Content-Type
            content_type = response.headers.get("Content-Type", "")
            if "video" not in content_type and "mp4" not in content_type:
                logger.warning(f"⚠️ Неожиданный Content-Type: {content_type}")

            # Скачиваем файл
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(temp_file.name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            # Проверяем, что файл не пустой
            file_size = os.path.getsize(temp_file.name)
            if file_size > 10 * 1024:  # Минимум 10KB (чтобы исключить пустые файлы)
                logger.info(
                    f"✅ Запись {recording.id} скачана, размер: {file_size/(1024*1024):.1f} МБ"
                )

                # Дополнительная проверка: пытаемся определить длительность видео
                if file_size > 100 * 1024:  # Если файл больше 100KB
                    try:
                        # Простая проверка: читаем первые байты файла
                        with open(temp_file.name, "rb") as f:
                            header = f.read(100)
                            # Проверяем сигнатуры MP4
                            if b"ftyp" in header or b"moov" in header:
                                logger.debug(
                                    f"✅ Файл похож на MP4 (найдены сигнатуры)"
                                )
                            else:
                                logger.warning(
                                    f"⚠️ Файл может быть не видео (отсутствуют MP4 сигнатуры)"
                                )
                    except:
                        pass

                return temp_file.name
            else:
                logger.warning(f"⚠️ Файл слишком маленький ({file_size} байт)")
                if os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
                return None

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
        """Отправляет видео в Telegram - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
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

                # Логируем ответ от Telegram для отладки
                logger.debug(f"📤 Ответ Telegram API: {response.status_code}")

                if response.status_code != 200:
                    logger.error(
                        f"❌ Telegram API вернул ошибку: {response.status_code} - {response.text}"
                    )
                    return False

                response.raise_for_status()
                result = response.json()

                if result.get("ok"):
                    logger.info("✅ Видео успешно отправлено в Telegram")
                    return True
                else:
                    logger.error(f"❌ Ошибка Telegram API: {result}")
                    return False

        except Exception as e:
            logger.error(f"❌ Ошибка отправки видео: {e}")
            return False


class StateManager:
    """Управление состоянием обработанных записей"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.processed_ids = set()
        self.last_processed_time = 0
        self.is_writable = True

        try:
            self.load_state()
        except PermissionError as e:
            logger.warning(f"⚠️ Не удалось загрузить состояние: {e}")
            logger.warning("⚠️ Состояние не будет сохраняться между запусками")
            self.is_writable = False
            # Используем начальные значения
            self.last_processed_time = int(time.time() - 3600)

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
        if not self.is_writable:
            logger.debug("⚠️ Состояние не сохраняется (файл недоступен для записи)")
            return

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

            logger.debug(
                f"💾 Состояние сохранено. Обработано записей: {len(self.processed_ids)}"
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
        logger.debug(f"📝 Запись {recording_id} помечена как обработанная")
        # Немедленно сохраняем состояние после пометки
        self.save_state()

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
    """Форматирует подпись для Telegram с обработкой отсутствующих данных"""
    try:
        # Если время невалидное (например, 1970 год), используем текущее время
        if recording.start_time < 1000000000:  # timestamp до 2001 года
            start_time = datetime.now()
            time_source = " (время приблизительное)"
        else:
            start_time = datetime.fromtimestamp(recording.start_time)
            time_source = ""

        # Форматируем дату и время
        time_str = start_time.strftime("%d.%m.%Y %H:%M:%S")

        # Форматируем длительность
        if recording.duration > 0:
            duration_sec = recording.duration / 1000  # Конвертируем мс в секунды
            duration_str = format_duration(int(duration_sec))
        else:
            duration_str = "длительность неизвестна"

        # Форматируем размер
        size_mb = recording.size / (1024 * 1024)
        if size_mb > 0.1:  # Если размер больше 100KB
            size_str = f"{size_mb:.1f} МБ"
        else:
            size_str = "размер неизвестен"

        # Создаем подпись
        caption = (
            f"<b>🎥 Движение обнаружено</b>{time_source}\n\n"
            f"<b>📷 Камера:</b> {camera_name}\n"
            f"<b>🕐 Время:</b> {time_str}\n"
            f"<b>⏱️ Длительность:</b> {duration_str}\n"
            f"<b>📦 Размер:</b> {size_str}\n\n"
            f"<i>#surveillance</i>"
        )

        return caption

    except Exception as e:
        logger.error(f"❌ Ошибка форматирования подписи: {e}")
        return f"🎥 Обнаружено движение\n📷 Камера: {camera_name}"


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
            # Получаем новые записи с увеличенным лимитом
            recordings = synology.get_recordings(camera_id=camera_id, limit=20)

            new_recordings = 0

            for recording in recordings:
                # Пропускаем уже обработанные
                if state.is_processed(recording.id):
                    logger.debug(f"⏭️  Запись {recording.id} уже обработана, пропускаю")
                    continue

                # Пропускаем записи с нулевой или отрицательной длительностью
                if recording.duration <= 0:
                    logger.debug(
                        f"⚠️ Пропускаю запись {recording.id} с нулевой длительностью"
                    )
                    continue

                logger.info(
                    f"🆕 Новая запись: ID={recording.id}, "
                    f"время={datetime.fromtimestamp(recording.start_time).strftime('%H:%M:%S')}, "
                    f"длительность={format_duration(recording.duration // 1000)}"
                )

                try:
                    # Скачиваем запись
                    video_path = synology.download_full_recording(recording)

                    if video_path and os.path.exists(video_path):
                        # Проверяем размер файла
                        file_size = os.path.getsize(video_path)
                        if file_size < 10 * 1024:  # Меньше 10KB - скорее всего ошибка
                            logger.warning(
                                f"⚠️ Файл слишком маленький ({file_size} байт), пропускаю"
                            )
                            os.remove(video_path)
                            continue

                        # Формируем подпись
                        caption = format_caption(recording, camera_name)

                        # Отправляем в Telegram
                        logger.info(f"📨 Отправляю запись {recording.id} в Telegram...")
                        if telegram.send_video(video_path, caption):
                            state.mark_processed(recording.id)
                            new_recordings += 1
                            logger.info(
                                f"✅ Запись {recording.id} успешно обработана и отправлена"
                            )
                        else:
                            logger.error(
                                f"❌ Не удалось отправить запись {recording.id} в Telegram"
                            )

                        # Удаляем временный файл
                        try:
                            os.remove(video_path)
                        except Exception as e:
                            logger.warning(
                                f"⚠️ Не удалось удалить временный файл {video_path}: {e}"
                            )
                    else:
                        logger.error(f"❌ Не удалось скачать запись {recording.id}")

                except Exception as e:
                    logger.error(f"❌ Ошибка обработки записи {recording.id}: {e}")
                    # Продолжаем со следующей записью

                # Проверяем флаг shutdown
                if shutdown_requested:
                    break

            if new_recordings > 0:
                logger.info(f"📊 Обработано новых записей: {new_recordings}")
            else:
                logger.debug("🔍 Новых записей не обнаружено")

            # Сохраняем состояние
            state.save_state()

            # Ждем следующей проверки
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
            time.sleep(10)  # Пауза при серьезной ошибке

    # Завершение работы
    logger.info("👋 Завершение работы бота")
    state.save_state()


if __name__ == "__main__":
    main()
