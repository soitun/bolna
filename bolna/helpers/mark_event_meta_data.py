import asyncio
import copy
import time

from bolna.helpers.logger_config import configure_logger

logger = configure_logger(__name__)

HIGH_DELAY_THRESHOLD = 2.0


class MarkEventMetaData:
    def __init__(self):
        self.mark_event_meta_data = {}
        self.previous_mark_event_meta_data = {}
        self.counter = 0
        self.mark_changed = asyncio.Event()

        # Mark tracking stats
        self._mark_stats = {
            "total_sent": 0,
            "total_acked": 0,
            "delays": [],
            "per_sequence": {},  # sequence_id -> {sent, acked, delays, interrupted}
        }

    def _ensure_sequence(self, sequence_id):
        if sequence_id not in self._mark_stats["per_sequence"]:
            self._mark_stats["per_sequence"][sequence_id] = {
                "sent": 0,
                "acked": 0,
                "delays": [],
                "interrupted": False,
            }

    def update_data(self, mark_id, value):
        value['counter'] = self.counter
        self.counter += 1
        self.mark_event_meta_data[mark_id] = value

        # Track sends for non-pre-mark messages
        if value.get("type") != "pre_mark_message":
            self._mark_stats["total_sent"] += 1
            seq = value.get("sequence_id")
            if seq is not None:
                self._ensure_sequence(seq)
                self._mark_stats["per_sequence"][seq]["sent"] += 1

    def record_ack(self, delay, sequence_id):
        self._mark_stats["total_acked"] += 1
        if delay >= 0:
            self._mark_stats["delays"].append(delay)
        if sequence_id is not None:
            self._ensure_sequence(sequence_id)
            entry = self._mark_stats["per_sequence"][sequence_id]
            entry["acked"] += 1
            if delay >= 0:
                entry["delays"].append(delay)

    def fetch_data(self, mark_id):
        result = self.mark_event_meta_data.pop(mark_id, {})
        if result:
            self.mark_changed.set()
        return result

    def clear_data(self):
        logger.info(f"Clearing mark meta data dict")
        self.counter = 0

        # Count remaining post-marks as missed, flag their sequences as interrupted
        for mark_id, value in self.mark_event_meta_data.items():
            if value.get("type") != "pre_mark_message":
                seq = value.get("sequence_id")
                if seq is not None and seq in self._mark_stats["per_sequence"]:
                    self._mark_stats["per_sequence"][seq]["interrupted"] = True

        self.previous_mark_event_meta_data = copy.deepcopy(self.mark_event_meta_data)
        self.mark_event_meta_data = {}
        self.mark_changed.set()

    def get_mark_tracking_summary(self):
        stats = self._mark_stats
        total_sent = stats["total_sent"]
        total_acked = stats["total_acked"]
        all_delays = stats["delays"]

        summary = {
            "total_sent": total_sent,
            "total_acked": total_acked,
            "total_missed": total_sent - total_acked,
            "max_delay_s": round(max(all_delays), 3) if all_delays else 0,
            "avg_delay_s": round(sum(all_delays) / len(all_delays), 3) if all_delays else 0,
            "high_delay_count": sum(1 for d in all_delays if d > HIGH_DELAY_THRESHOLD),
            "per_sequence": [],
        }

        for seq_id in sorted(stats["per_sequence"].keys()):
            seq = stats["per_sequence"][seq_id]
            seq_delays = seq["delays"]
            summary["per_sequence"].append({
                "seq": seq_id,
                "sent": seq["sent"],
                "acked": seq["acked"],
                "max_delay": round(max(seq_delays), 3) if seq_delays else 0,
                "avg_delay": round(sum(seq_delays) / len(seq_delays), 3) if seq_delays else 0,
                "interrupted": seq["interrupted"],
            })

        return summary

    def fetch_cleared_mark_event_data(self):
        return self.previous_mark_event_meta_data

    def __str__(self):
        return f"{self.mark_event_meta_data}"
