"""Tests for the rewritten bot (v2)."""

import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from main import (
    AppConfig,
    RecordingProgress,
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


def _make_fake_video(tmp_path: Path, name="fake.mp4") -> str:
    """Create a dummy file so os.path.getsize() succeeds in tests."""
    p = tmp_path / name
    p.write_bytes(b"\x00" * 4096)
    return str(p)


def _mock_syno(video_path: str) -> MagicMock:
    s = MagicMock()
    s.download_fragment.return_value = video_path
    return s


def _mock_tg(ok=True) -> MagicMock:
    t = MagicMock()
    t.send_video.return_value = ok
    return t


def _proc(synology, telegram, state, rec, config):
    return process_recording(synology, telegram, state, rec, "Cam1", config)


# ---------------------------------------------------------------------------
# StateManager: basic persistence
# ---------------------------------------------------------------------------

class TestStateManager:
    def test_fresh_state_is_empty(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        assert len(state.progress) == 0
        assert len(state.completed_ids) == 0

    def test_save_and_reload(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["42"] = RecordingProgress(
            recording_id="42",
            next_offset_ms=5000,
            fragments_sent=3,
            known_duration_ms=30000,
            cycles_at_end=1,
        )
        state.save()

        state2 = StateManager(config)
        p = state2.progress["42"]
        assert p.next_offset_ms == 5000
        assert p.fragments_sent == 3
        assert p.known_duration_ms == 30000
        assert p.cycles_at_end == 1

    def test_mark_sent_advances_offset(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["1"] = RecordingProgress(recording_id="1", next_offset_ms=0)

        state.mark_sent("1", 10000)

        p = state.progress["1"]
        assert p.next_offset_ms == 10000
        assert p.fragments_sent == 1
        assert p.consecutive_fails == 0
        assert p.cycles_at_end == 0

    def test_mark_failed_does_not_advance_offset(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["1"] = RecordingProgress(recording_id="1", next_offset_ms=5000)

        state.mark_failed("1")

        assert state.progress["1"].next_offset_ms == 5000
        assert state.progress["1"].consecutive_fails == 1

    def test_mark_completed(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.progress["99"] = RecordingProgress(recording_id="99")

        state.mark_completed("99", reason="test")

        assert state.is_completed("99")
        assert "99" in state.completed_ids

    def test_completed_ids_persist(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.mark_completed("55", reason="test")

        state2 = StateManager(config)
        assert state2.is_completed("55")


# ---------------------------------------------------------------------------
# process_recording: max_fragments_per_cycle
# ---------------------------------------------------------------------------

class TestMaxFragmentsPerCycle:
    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_stops_after_max_fragments(self, mock_rm, mock_dur, tmp_path):
        config = _make_config(tmp_path, max_fragments_per_cycle=3)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        sent = _proc(synology, telegram, state, rec, config)

        assert sent == 3
        assert telegram.send_video.call_count == 3

    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_resumes_from_correct_offset_next_cycle(self, mock_rm, mock_dur, tmp_path):
        config = _make_config(tmp_path, max_fragments_per_cycle=2)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        # First cycle: 2 fragments, each 10s
        _proc(synology, telegram, state, rec, config)
        assert state.progress["1"].next_offset_ms == 20000

        # Second cycle: continues from 20000ms
        _proc(synology, telegram, state, rec, config)
        assert state.progress["1"].next_offset_ms == 40000


# ---------------------------------------------------------------------------
# process_recording: offset advances correctly
# ---------------------------------------------------------------------------

class TestOffsetAdvancement:
    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_offset_advances_by_actual_duration(self, mock_rm, mock_dur, tmp_path):
        config = _make_config(tmp_path, max_fragments_per_cycle=1)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        _proc(synology, telegram, state, rec, config)

        assert state.progress["1"].next_offset_ms == 10000

    @patch("main.get_video_duration", return_value=(7.5, True))
    @patch("os.remove")
    def test_offset_advances_by_fractional_duration(self, mock_rm, mock_dur, tmp_path):
        config = _make_config(tmp_path, max_fragments_per_cycle=1)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        _proc(synology, telegram, state, rec, config)

        assert state.progress["1"].next_offset_ms == 7500


# ---------------------------------------------------------------------------
# process_recording: completion detection
# ---------------------------------------------------------------------------

class TestCompletionDetection:
    def test_marks_completed_when_stable(self, tmp_path):
        """Recording completes when offset >= duration AND cycles_at_end >= end_stable_cycles."""
        config = _make_config(tmp_path, end_stable_cycles=2)
        state = StateManager(config)

        state.progress["5"] = RecordingProgress(
            recording_id="5",
            next_offset_ms=30000,
            known_duration_ms=30000,
            cycles_at_end=2,
        )
        rec = _make_recording(rec_id="5", duration_s=30)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        with patch("main.get_video_duration", return_value=(10.0, True)), \
             patch("os.remove"):
            _proc(synology, telegram, state, rec, config)

        assert state.is_completed("5")
        assert telegram.send_video.call_count == 0

    @patch("main.get_video_duration", return_value=(10.0, True))
    @patch("os.remove")
    def test_not_completed_when_duration_grows(self, mock_rm, mock_dur, tmp_path):
        """If API reports longer duration, cycles_at_end resets and recording is NOT completed."""
        config = _make_config(tmp_path, end_stable_cycles=2)
        state = StateManager(config)

        state.progress["5"] = RecordingProgress(
            recording_id="5",
            next_offset_ms=30000,
            known_duration_ms=30000,
            cycles_at_end=2,
        )
        # API now reports longer recording
        rec = _make_recording(rec_id="5", duration_s=60)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        _proc(synology, telegram, state, rec, config)

        assert not state.is_completed("5")
        # cycles_at_end was reset to 0 when duration grew, then may have incremented
        # once within this cycle as the loop reached the new end — but must be < threshold
        assert state.progress["5"].cycles_at_end < config.end_stable_cycles, \
            "must not reach stable end after duration grew"

    def test_increments_cycles_at_end(self, tmp_path):
        """When at end but cycles_at_end < threshold, cycles_at_end increments."""
        config = _make_config(tmp_path, end_stable_cycles=3)
        state = StateManager(config)

        state.progress["5"] = RecordingProgress(
            recording_id="5",
            next_offset_ms=30000,
            known_duration_ms=30000,
            cycles_at_end=1,
        )
        rec = _make_recording(rec_id="5", duration_s=30)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        with patch("main.get_video_duration", return_value=(10.0, True)), \
             patch("os.remove"):
            sent = _proc(synology, telegram, state, rec, config)

        assert sent == 0
        assert not state.is_completed("5")
        assert state.progress["5"].cycles_at_end == 2


# ---------------------------------------------------------------------------
# process_recording: live edge (empty / short fragment)
# ---------------------------------------------------------------------------

class TestLiveEdge:
    @patch("main.get_video_duration", return_value=(0.1, True))
    @patch("os.remove")
    def test_short_fragment_stops_cycle(self, mock_rm, mock_dur, tmp_path):
        """Fragment shorter than MIN_VALID_FRAGMENT_S -> live edge, don't send."""
        config = _make_config(tmp_path)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)
        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        sent = _proc(synology, telegram, state, rec, config)

        assert sent == 0
        assert telegram.send_video.call_count == 0

    @patch("os.remove")
    def test_no_fragment_file_stops_cycle(self, mock_rm, tmp_path):
        """download_fragment returning None stops the inner loop."""
        config = _make_config(tmp_path)
        state = StateManager(config)
        rec = _make_recording(rec_id="1", duration_s=3600)

        synology = MagicMock()
        synology.download_fragment.return_value = None
        telegram = _mock_tg()

        sent = _proc(synology, telegram, state, rec, config)

        assert sent == 0
        assert state.progress["1"].consecutive_fails == 1


# ---------------------------------------------------------------------------
# process_recording: already completed
# ---------------------------------------------------------------------------

class TestAlreadyCompleted:
    def test_skips_completed_recording(self, tmp_path):
        config = _make_config(tmp_path)
        state = StateManager(config)
        state.completed_ids.add("99")

        video = _make_fake_video(tmp_path)
        synology = _mock_syno(video)
        telegram = _mock_tg()
        rec = _make_recording(rec_id="99")

        sent = _proc(synology, telegram, state, rec, config)

        assert sent == 0
        assert synology.download_fragment.call_count == 0


# ---------------------------------------------------------------------------
# TelegramBot: 429 retry
# ---------------------------------------------------------------------------

class TestTelegramRateLimit:
    def _make_bot(self):
        from main import TelegramBot
        bot = TelegramBot.__new__(TelegramBot)
        bot.token = "x"
        bot.chat_id = "1"
        bot.base_url = "https://api.telegram.org/botx"
        bot.bot_name = "TestBot"
        return bot

    def test_retries_on_429_then_succeeds(self, tmp_path):
        bot = self._make_bot()

        rate_limit = MagicMock()
        rate_limit.status_code = 429
        rate_limit.json.return_value = {
            "ok": False,
            "parameters": {"retry_after": 1},
        }
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"ok": True}

        session = MagicMock()
        session.post.side_effect = [rate_limit, ok_resp]
        bot.session = session

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"fake")

        with patch("main.time.sleep") as mock_sleep:
            result = bot.send_video(str(video_path))

        assert result is True
        assert session.post.call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_returns_false_after_all_429s(self, tmp_path):
        bot = self._make_bot()

        rate_limit = MagicMock()
        rate_limit.status_code = 429
        rate_limit.json.return_value = {
            "ok": False,
            "parameters": {"retry_after": 1},
        }

        session = MagicMock()
        session.post.side_effect = [rate_limit, rate_limit, rate_limit]
        bot.session = session

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"fake")

        with patch("main.time.sleep"):
            result = bot.send_video(str(video_path))

        assert result is False
