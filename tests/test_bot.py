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
# process_recording: API loop detection (short recordings / oversized fragments)
# ---------------------------------------------------------------------------

class TestApiLoopDetection:
    """
    Synology returns the full recording regardless of offsetTimeMs when the
    offset is past the recording length. The bot must detect this BEFORE
    sending to avoid duplicate fragments.
    """

    @patch("os.remove")
    def test_detects_loop_by_offset_past_video_length(self, mock_rm, tmp_path):
        """
        offset > actual_video_length AND actual > requested*1.05 → loop detected,
        recording completed WITHOUT sending the duplicate.
        """
        config = _make_config(tmp_path, fragment_duration_ms=10000)
        state = StateManager(config)
        rec = _make_recording(rec_id="7", duration_s=0)  # API returns duration=0

        # Bot is already at offset 50000ms but recording is only 11.856s
        state.progress["7"] = RecordingProgress(
            recording_id="7",
            next_offset_ms=50000,
            known_duration_ms=0,
        )

        video = _make_fake_video(tmp_path, "loop.mp4")
        video_size = os.path.getsize(video)
        synology = _mock_syno(video)
        telegram = _mock_tg()

        # Synology returns 11.856s even though we requested 10s (oversized, full recording)
        with patch("main.get_video_duration", return_value=(11.856, True)):
            sent = _proc(synology, telegram, state, rec, config)

        assert sent == 0
        assert telegram.send_video.call_count == 0, "must NOT send a duplicate"
        assert state.is_completed("7"), "must mark completed"

    @patch("os.remove")
    def test_detects_loop_by_repeated_file_size(self, mock_rm, tmp_path):
        """
        When known_duration_ms is already set but Synology returns the same oversized
        file for two consecutive offsets (actual recording is shorter than believed),
        the second fragment triggers the repeated-size loop detection.

        Scenario: bot believes recording is 50s, but actual recording is 11.856s.
          - Fragment 1 at offset=0: oversized (11.856s > 10s), not past end (0 < 11856ms).
            known_duration already set so no inference. Sent, offset advances to 11856ms.
          - Fragment 2 at offset=11856ms: same file, same size, oversized.
            repeated-size check fires → mark completed WITHOUT sending.
        """
        config = _make_config(tmp_path, fragment_duration_ms=10000, max_fragments_per_cycle=5)
        state = StateManager(config)
        rec = _make_recording(rec_id="8", duration_s=0)

        # Bot believes recording is 50s, but Synology loops at 11.856s
        state.progress["8"] = RecordingProgress(
            recording_id="8",
            next_offset_ms=0,
            known_duration_ms=50000,
        )

        video = _make_fake_video(tmp_path, "loop2.mp4")
        synology = _mock_syno(video)
        telegram = _mock_tg()

        with patch("main.get_video_duration", return_value=(11.856, True)):
            sent = _proc(synology, telegram, state, rec, config)

        assert sent == 1, "only first fragment should be sent"
        assert telegram.send_video.call_count == 1
        assert state.is_completed("8")

    @patch("os.remove")
    def test_normal_short_last_fragment_not_treated_as_loop(self, mock_rm, tmp_path):
        """
        A legitimately short last fragment (actual < requested) must NOT trigger loop detection.
        """
        config = _make_config(tmp_path, fragment_duration_ms=10000, end_stable_cycles=2)
        state = StateManager(config)
        rec = _make_recording(rec_id="9", duration_s=25)

        # Recording is 25s, we're at offset 20s, last chunk is 5s (<10s requested)
        state.progress["9"] = RecordingProgress(
            recording_id="9",
            next_offset_ms=20000,
            known_duration_ms=25000,
        )

        video = _make_fake_video(tmp_path, "last_chunk.mp4")
        synology = _mock_syno(video)
        telegram = _mock_tg()

        with patch("main.get_video_duration", return_value=(5.0, True)):
            sent = _proc(synology, telegram, state, rec, config)

        assert sent == 1, "last valid fragment must be sent"
        assert not state.is_completed("9"), "must not be completed yet (needs stable cycles)"


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
