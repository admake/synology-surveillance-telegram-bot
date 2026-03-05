"""Tests for fragment deduplication, rate-limit handling, and cycle limits."""

import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from main import (
    AppConfig,
    FragmentProgress,
    Recording,
    StateManager,
    process_recording,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(tmp_path, **kwargs) -> AppConfig:
    c = AppConfig()
    c.state_file = str(tmp_path / "state.json")
    for k, v in kwargs.items():
        setattr(c, k, v)
    return c


def _make_recording(rec_id="1", duration_s=100) -> Recording:
    return Recording(
        id=rec_id,
        camera_id="1",
        start_time=int(time.time()) - duration_s,
        duration=duration_s,
        size=0,
    )


def _make_state(config: AppConfig, progress: dict[str, FragmentProgress] | None = None) -> StateManager:
    state = StateManager(config)
    if progress:
        state.progress = progress
    return state


# ---------------------------------------------------------------------------
# StateManager: crash-recovery guard
# ---------------------------------------------------------------------------

class TestCrashRecoveryGuard:
    def test_advances_offset_when_in_progress_set(self, tmp_path):
        """If in_progress_offset_ms > next_offset_ms on load, offset must advance."""
        config = _make_config(tmp_path)
        state_data = {
            "completed_ids": [],
            "progress": {
                "42": {
                    "next_offset_ms": 10000,
                    "fragments_sent": 1,
                    "last_attempt_time": 0,
                    "consecutive_fails": 0,
                    "is_completed": False,
                    "known_duration_ms": 30000,
                    "at_end_since_duration_ms": 0,
                    "last_seen_time": time.time(),
                    "in_progress_offset_ms": 17500,  # crashed mid-send
                }
            },
        }
        Path(config.state_file).parent.mkdir(parents=True, exist_ok=True)
        Path(config.state_file).write_text(json.dumps(state_data))

        state = StateManager(config)

        p = state.progress["42"]
        assert p.next_offset_ms == 17500, "offset must be advanced to in_progress value"
        assert p.in_progress_offset_ms == -1, "in_progress must be cleared after recovery"

    def test_no_change_when_in_progress_not_set(self, tmp_path):
        """Normal state (in_progress == -1) must not alter next_offset_ms."""
        config = _make_config(tmp_path)
        state_data = {
            "completed_ids": [],
            "progress": {
                "42": {
                    "next_offset_ms": 10000,
                    "fragments_sent": 1,
                    "last_attempt_time": 0,
                    "consecutive_fails": 0,
                    "is_completed": False,
                    "known_duration_ms": 30000,
                    "at_end_since_duration_ms": 0,
                    "last_seen_time": time.time(),
                    "in_progress_offset_ms": -1,
                }
            },
        }
        Path(config.state_file).parent.mkdir(parents=True, exist_ok=True)
        Path(config.state_file).write_text(json.dumps(state_data))

        state = StateManager(config)

        assert state.progress["42"].next_offset_ms == 10000

    def test_in_progress_cleared_on_successful_send(self, tmp_path):
        """mark_fragment_sent must clear in_progress_offset_ms."""
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["7"] = FragmentProgress(
            recording_id="7",
            next_offset_ms=5000,
            in_progress_offset_ms=15000,
        )

        state.mark_fragment_sent("7", 15000)

        assert state.progress["7"].in_progress_offset_ms == -1
        assert state.progress["7"].next_offset_ms == 15000

    def test_in_progress_cleared_on_failed_send(self, tmp_path):
        """mark_fragment_failed must clear in_progress_offset_ms so next cycle retries."""
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["7"] = FragmentProgress(
            recording_id="7",
            next_offset_ms=5000,
            in_progress_offset_ms=15000,
        )

        state.mark_fragment_failed("7")

        assert state.progress["7"].in_progress_offset_ms == -1
        assert state.progress["7"].next_offset_ms == 5000, "offset must NOT advance on failure"


# ---------------------------------------------------------------------------
# process_recording: max_fragments_per_cycle
# ---------------------------------------------------------------------------

class TestMaxFragmentsPerCycle:
    def _make_mocks(self, actual_duration=10.0, send_ok=True):
        synology = MagicMock()
        synology.download_fragment.return_value = "/tmp/fake.mp4"
        telegram = MagicMock()
        telegram.send_video.return_value = send_ok
        return synology, telegram

    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_stops_after_max_fragments(self, mock_rm, mock_dur, tmp_path):
        """Inner loop must stop after max_fragments_per_cycle successful sends."""
        config = _make_config(tmp_path, max_fragments_per_cycle=3)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)  # very long recording
        synology, telegram = self._make_mocks()

        sent = process_recording(
            synology, telegram, state, rec,
            camera_name="Cam1",
            fragment_duration_ms=10000,
            max_consecutive_fails=3,
            max_fragments_per_cycle=3,
        )

        assert sent == 3
        assert telegram.send_video.call_count == 3

    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_resumes_from_correct_offset_next_cycle(self, mock_rm, mock_dur, tmp_path):
        """After hitting the cycle limit, next call continues from saved offset."""
        config = _make_config(tmp_path, max_fragments_per_cycle=2)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        synology, telegram = self._make_mocks()

        # First cycle
        process_recording(
            synology, telegram, state, rec,
            camera_name="Cam1",
            fragment_duration_ms=10000,
            max_consecutive_fails=3,
            max_fragments_per_cycle=2,
        )

        first_cycle_offset = state.progress["1"].next_offset_ms
        assert first_cycle_offset == 20000  # 2 × 10s

        # Second cycle
        process_recording(
            synology, telegram, state, rec,
            camera_name="Cam1",
            fragment_duration_ms=10000,
            max_consecutive_fails=3,
            max_fragments_per_cycle=2,
        )

        assert state.progress["1"].next_offset_ms == 40000  # 4 × 10s


# ---------------------------------------------------------------------------
# process_recording: no duplicate send on same offset
# ---------------------------------------------------------------------------

class TestNoDuplicateSend:
    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_pre_commit_persisted_before_send(self, mock_rm, mock_dur, tmp_path):
        """in_progress_offset_ms is written to state before telegram.send_video is called."""
        config = _make_config(tmp_path, max_fragments_per_cycle=1)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=100)

        synology = MagicMock()
        synology.download_fragment.return_value = "/tmp/fake.mp4"

        captured_in_progress = {}

        def fake_send(path, caption=""):
            captured_in_progress["val"] = state.progress["1"].in_progress_offset_ms
            return True

        telegram = MagicMock()
        telegram.send_video.side_effect = fake_send

        process_recording(
            synology, telegram, state, rec,
            camera_name="Cam1",
            fragment_duration_ms=10000,
            max_consecutive_fails=3,
            max_fragments_per_cycle=1,
        )

        assert captured_in_progress["val"] == 10000, \
            "in_progress_offset_ms must be set before send_video is called"


# ---------------------------------------------------------------------------
# StateManager: completion detection
# ---------------------------------------------------------------------------

class TestCompletionDetection:
    def test_marks_completed_when_at_end_and_stable(self, tmp_path):
        """Recording is completed only once offset >= duration AND duration is stable."""
        config = _make_config(tmp_path)
        state = StateManager(config)
        rec = _make_recording(rec_id="5", duration_s=30)

        state.progress["5"] = FragmentProgress(
            recording_id="5",
            next_offset_ms=30000,
            known_duration_ms=30000,
            at_end_since_duration_ms=30000,  # was at end last cycle with same duration
        )

        with patch("main.get_video_duration", return_value=(10.0, True)), \
             patch("os.remove"):
            synology = MagicMock()
            synology.download_fragment.return_value = "/tmp/fake.mp4"
            telegram = MagicMock()
            telegram.send_video.return_value = True

            process_recording(
                synology, telegram, state, rec,
                camera_name="Cam1",
                fragment_duration_ms=10000,
                max_consecutive_fails=3,
            )

        assert state.is_completed("5")

    def test_does_not_complete_when_duration_grew(self, tmp_path):
        """If recording duration grew, must NOT mark as completed."""
        config = _make_config(tmp_path)
        state = StateManager(config)

        state.progress["5"] = FragmentProgress(
            recording_id="5",
            next_offset_ms=30000,
            known_duration_ms=30000,
            at_end_since_duration_ms=30000,
        )

        # API now reports longer duration
        rec = _make_recording(rec_id="5", duration_s=60)

        with patch("main.get_video_duration", return_value=(10.0, True)), \
             patch("os.remove"):
            synology = MagicMock()
            synology.download_fragment.return_value = "/tmp/fake.mp4"
            telegram = MagicMock()
            telegram.send_video.return_value = True

            process_recording(
                synology, telegram, state, rec,
                camera_name="Cam1",
                fragment_duration_ms=10000,
                max_consecutive_fails=3,
                max_fragments_per_cycle=1,
            )

        assert not state.is_completed("5"), "must not complete when duration grew"
        assert state.progress["5"].at_end_since_duration_ms == 0, \
            "stability marker must reset when duration grows"


# ---------------------------------------------------------------------------
# TelegramBot: 429 retry
# ---------------------------------------------------------------------------

class TestTelegramRateLimit:
    def test_retries_once_on_429(self, tmp_path):
        """send_video must sleep retry_after seconds and retry once on 429."""
        from main import TelegramBot

        bot = TelegramBot.__new__(TelegramBot)
        bot.token = "x"
        bot.chat_id = "1"
        bot.base_url = "https://api.telegram.org/botx"
        bot.bot_name = "TestBot"

        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.json.return_value = {
            "ok": False,
            "error_code": 429,
            "parameters": {"retry_after": 1},
        }

        ok_response = MagicMock()
        ok_response.status_code = 200
        ok_response.json.return_value = {"ok": True}

        session = MagicMock()
        session.post.side_effect = [rate_limit_response, ok_response]
        bot.session = session

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"fake")

        with patch("main.time.sleep") as mock_sleep:
            result = bot.send_video(str(video_path))

        assert result is True
        assert session.post.call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_returns_false_after_second_failure(self, tmp_path):
        """send_video returns False if the retry also fails."""
        from main import TelegramBot

        bot = TelegramBot.__new__(TelegramBot)
        bot.token = "x"
        bot.chat_id = "1"
        bot.base_url = "https://api.telegram.org/botx"
        bot.bot_name = "TestBot"

        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.json.return_value = {
            "ok": False,
            "error_code": 429,
            "parameters": {"retry_after": 1},
        }

        session = MagicMock()
        session.post.side_effect = [rate_limit_response, rate_limit_response]
        bot.session = session

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"fake")

        with patch("main.time.sleep"):
            result = bot.send_video(str(video_path))

        assert result is False
