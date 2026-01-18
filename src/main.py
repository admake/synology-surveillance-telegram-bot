#!/usr/bin/env python3
"""
Адаптированная версия на основе работающего кода из старого контейнера
"""

import os
import json
import time
import logging
import signal
from datetime import datetime
from pathlib import Path

import requests
from requests.exceptions import RequestException
import telebot

# Настройка логирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


class SynologySurveillance:
    """Класс для работы с Surveillance Station API (адаптированный)"""

    def __init__(self):
        self.syno_ip = os.getenv("SYNO_IP")
        self.syno_port = os.getenv("SYNO_PORT", "5001")
        self.syno_login = os.getenv("SYNO_USER")
        self.syno_pass = os.getenv("SYNO_PASS")
        self.syno_otp = os.getenv("SYNO_OTP", None)

        self.base_url = f"https://{self.syno_ip}:{self.syno_port}/webapi/entry.cgi"
        self.session = requests.Session()
        self.session.verify = os.getenv("SSL_VERIFY", "false").lower() == "true"

        self.sid = None
        self.config_file = os.getenv("STATE_FILE", "/data/state.json")
        self.cameras_config = {}

    def login(self):
        """Аутентификация в Surveillance Station"""
        try:
            params = {
                "api": "SYNO.API.Auth",
                "version": "7",
                "method": "login",
                "account": self.syno_login,
                "passwd": self.syno_pass,
                "session": "SurveillanceStation",
                "format": "cookie",
            }

            if self.syno_otp:
                params["otp_code"] = self.syno_otp

            response = self.session.get(self.base_url, params=params, timeout=10)
            data = response.json()

            if data.get("success"):
                self.sid = data["data"]["sid"]
                logger.info(f"Authentication successful. SID: {self.sid[:15]}...")

                # Сохраняем конфигурацию камер
                self.save_cameras_config()
                return True
            else:
                logger.error(f"Authentication failed: {data}")
                return False

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def save_cameras_config(self):
        """Получает и сохраняет конфигурацию камер"""
        try:
            params = {
                "api": "SYNO.SurveillanceStation.Camera",
                "_sid": self.sid,
                "version": "9",
                "method": "List",
            }

            response = self.session.get(self.base_url, params=params, timeout=10)
            data = response.json()

            if data.get("success"):
                cameras = data.get("data", {}).get("cameras", [])
                self.cameras_config = {}

                for cam in cameras:
                    self.cameras_config[cam["id"]] = {
                        "id": cam["id"],
                        "name": cam.get("newName", cam.get("name", "Unknown")),
                        "ip": cam.get("ip", "N/A"),
                        "model": cam.get("model", "N/A"),
                    }

                # Сохраняем в файл
                config_data = {
                    "cameras": self.cameras_config,
                    "sid": self.sid,
                    "updated": datetime.now().isoformat(),
                }

                with open(self.config_file, "w") as f:
                    json.dump(config_data, f, indent=2)

                logger.info(f"Saved config for {len(cameras)} cameras")
                return True

        except Exception as e:
            logger.error(f"Error saving camera config: {e}")

        return False

    def get_last_recording_id(self, camera_id):
        """Получает ID последней записи для камеры"""
        try:
            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "_sid": self.sid,
                "version": "6",
                "method": "List",
                "cameraIds": str(camera_id),
                "limit": "1",
                "offset": "0",
                "fromTime": "0",
                "toTime": "0",
            }

            response = self.session.get(self.base_url, params=params, timeout=10)
            data = response.json()

            if data.get("success") and data["data"].get("recordings"):
                recording_id = data["data"]["recordings"][0]["id"]
                logger.debug(
                    f"Last recording ID for camera {camera_id}: {recording_id}"
                )
                return recording_id

        except Exception as e:
            logger.error(f"Error getting last recording: {e}")

        return None

    def download_recording(self, recording_id, offset_ms=0, duration_ms=10000):
        """Скачивает фрагмент записи"""
        try:
            download_url = f"{self.base_url}/temp.mp4"

            params = {
                "api": "SYNO.SurveillanceStation.Recording",
                "method": "Download",
                "version": "6",
                "_sid": self.sid,
                "id": recording_id,
                "mountId": "0",
                "offsetTimeMs": str(offset_ms),
                "playTimeMs": str(duration_ms),
            }

            response = self.session.get(
                download_url, params=params, stream=True, timeout=30
            )
            response.raise_for_status()

            # Создаем временный файл
            temp_file = f"/tmp/rec_{recording_id}_{int(time.time())}.mp4"

            with open(temp_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded recording {recording_id} to {temp_file}")
            return temp_file

        except Exception as e:
            logger.error(f"Error downloading recording {recording_id}: {e}")
            return None


class TelegramBot:
    """Класс для работы с Telegram"""

    def __init__(self):
        self.token = os.getenv("TG_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.bot = telebot.TeleBot(self.token)

    def send_video(self, video_path, camera_name="Camera"):
        """Отправляет видео в Telegram"""
        try:
            caption = (
                f"📹 {camera_name}\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            with open(video_path, "rb") as video_file:
                self.bot.send_video(self.chat_id, video_file, caption=caption)

            logger.info(f"Video sent to Telegram: {video_path}")
            return True

        except Exception as e:
            logger.error(f"Error sending video: {e}")
            return False


def main():
    """Основная функция - адаптированный вариант"""
    logger.info("Starting adapted Surveillance Station to Telegram Bot")

    # Проверка обязательных переменных
    required_vars = ["SYNO_IP", "SYNO_USER", "SYNO_PASS", "TG_TOKEN", "TG_CHAT_ID"]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            return

    camera_id = os.getenv("CAMERA_ID", "5")
    check_interval = int(os.getenv("CHECK_INTERVAL", "30"))

    # Инициализация
    synology = SynologySurveillance()
    telegram = TelegramBot()

    # Аутентификация
    if not synology.login():
        logger.error("Failed to authenticate to Surveillance Station")
        return

    # Загружаем конфигурацию камер
    camera_name = synology.cameras_config.get(camera_id, {}).get(
        "name", f"Camera {camera_id}"
    )
    logger.info(f"Monitoring camera: {camera_name} (ID: {camera_id})")

    # Отслеживаем последние записи
    last_recording_id = None
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        logger.info(f"Received signal {signum}, shutting down")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Starting monitoring loop...")

    while not shutdown_requested:
        try:
            # Получаем ID последней записи
            current_recording_id = synology.get_last_recording_id(camera_id)

            if current_recording_id and current_recording_id != last_recording_id:
                logger.info(f"New recording detected: {current_recording_id}")

                # Скачиваем первые 10 секунд
                video_file = synology.download_recording(current_recording_id, 0, 10000)

                if video_file:
                    # Отправляем в Telegram
                    if telegram.send_video(video_file, camera_name):
                        last_recording_id = current_recording_id
                        logger.info(
                            f"Successfully sent recording {current_recording_id}"
                        )

                    # Удаляем временный файл
                    try:
                        os.remove(video_file)
                    except:
                        pass
                else:
                    logger.error(f"Failed to download recording {current_recording_id}")

            # Ждем перед следующей проверкой
            for _ in range(check_interval):
                if shutdown_requested:
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(10)

    logger.info("Bot stopped")


if __name__ == "__main__":
    main()
