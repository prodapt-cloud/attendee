import json
import logging
from collections import Counter
from threading import Lock
from typing import Callable

from bots.bot_controller.bot_websocket_client import BotWebsocketClient

logger = logging.getLogger(__name__)

ATTENDEE_SENT_AUDIO_MESSAGES_METRIC = "attendee_sent_audio_messages_total"
ATTENDEE_AUDIO_SEND_FAILURES_METRIC = "attendee_audio_send_failures_total"
ATTENDEE_AUDIO_DROPPED_MESSAGES_METRIC = "attendee_audio_dropped_messages_total"
MIXED_AUDIO_TRIGGER = "realtime_audio.mixed"
PER_PARTICIPANT_AUDIO_TRIGGER = "realtime_audio.per_participant"


class BotWebsocketClientManager:
    """
    Manages BotWebsocketClient instances for mixed and per-participant audio / video
    websocket streams. When both URLs are identical, a single underlying client
    is shared to avoid duplicate connections. Callers just call send_mixed_audio
    / send_per_participant_audio / send_per_participant_video and this class handles client lifecycle.
    """

    def __init__(
        self,
        mixed_audio_url: str | None,
        per_participant_audio_url: str | None,
        per_participant_video_url: str | None,
        on_message_callback: Callable[[dict], None],
    ):
        def add_purpose(url: str, purpose: str):
            if not url:
                return
            if url not in self._url_to_purposes:
                self._url_to_purposes[url] = []
            self._url_to_purposes[url].append(purpose)

        self._sent_audio_messages_total = Counter()
        self._audio_send_failures_total = Counter()
        self._audio_dropped_messages_total = Counter()
        self._metric_lock = Lock()

        def get_or_create_client(url: str | None, purpose: str) -> BotWebsocketClient | None:
            if not url:
                return None
            if url not in client_by_url:
                client_by_url[url] = BotWebsocketClient(
                    url=url,
                    on_message_callback=on_message_callback,
                    on_send_success_callback=self._on_message_sent,
                    on_send_failure_callback=self._on_message_send_failed,
                    on_drop_callback=self._on_message_dropped,
                )
            add_purpose(url, purpose)
            return client_by_url[url]

        client_by_url: dict[str, BotWebsocketClient] = {}
        self._url_to_purposes: dict[str, list[str]] = {}
        self._mixed_audio_client = get_or_create_client(mixed_audio_url, "mixed_audio")
        self._per_participant_audio_client = get_or_create_client(per_participant_audio_url, "per_participant_audio")
        self._per_participant_video_client = get_or_create_client(per_participant_video_url, "per_participant_video")
        self._clients = list(client_by_url.values())

    def _audio_message_labels(self, message: dict):
        trigger = message.get("trigger")
        if trigger not in [MIXED_AUDIO_TRIGGER, PER_PARTICIPANT_AUDIO_TRIGGER]:
            return None

        data = message.get("data") or {}
        stream = "mixed" if trigger == MIXED_AUDIO_TRIGGER else "per_participant"
        participant_uuid = str(data.get("participant_uuid")) if data.get("participant_uuid") is not None else None
        return message.get("bot_id"), stream, participant_uuid

    def _audio_message_log_payload(self, message: dict, metric: str, total: int, **extra):
        data = message.get("data") or {}
        labels = self._audio_message_labels(message)
        if labels is None:
            return None

        bot_id, stream, participant_uuid = labels
        payload = {
            "metric": metric,
            "value": 1,
            "total": total,
            "bot_id": bot_id,
            "stream": stream,
            "participant_uuid": participant_uuid,
            "sequence": data.get("sequence"),
        }
        payload.update(extra)
        return payload

    def _on_message_sent(self, message: dict):
        labels = self._audio_message_labels(message)
        if labels is None:
            return

        with self._metric_lock:
            self._sent_audio_messages_total[labels] += 1
            total = self._sent_audio_messages_total[labels]

        logger.info(json.dumps(self._audio_message_log_payload(message, ATTENDEE_SENT_AUDIO_MESSAGES_METRIC, total)))

    def _on_message_send_failed(self, message: dict, exception: Exception):
        labels = self._audio_message_labels(message)
        if labels is None:
            return

        with self._metric_lock:
            self._audio_send_failures_total[labels] += 1
            total = self._audio_send_failures_total[labels]

        logger.error(
            json.dumps(
                self._audio_message_log_payload(
                    message,
                    ATTENDEE_AUDIO_SEND_FAILURES_METRIC,
                    total,
                    exception_type=exception.__class__.__name__,
                    exception_message=str(exception),
                )
            )
        )

    def _on_message_dropped(self, message: dict, connection_state: str):
        labels = self._audio_message_labels(message)
        if labels is None:
            return

        with self._metric_lock:
            self._audio_dropped_messages_total[labels] += 1
            total = self._audio_dropped_messages_total[labels]

        logger.warning(
            json.dumps(
                self._audio_message_log_payload(
                    message,
                    ATTENDEE_AUDIO_DROPPED_MESSAGES_METRIC,
                    total,
                    connection_state=connection_state,
                )
            )
        )

    def sent_audio_messages_total(self, bot_id: str, stream: str, participant_uuid: str | None = None) -> int:
        return self._sent_audio_messages_total[(bot_id, stream, participant_uuid)]

    def audio_send_failures_total(self, bot_id: str, stream: str, participant_uuid: str | None = None) -> int:
        return self._audio_send_failures_total[(bot_id, stream, participant_uuid)]

    def audio_dropped_messages_total(self, bot_id: str, stream: str, participant_uuid: str | None = None) -> int:
        return self._audio_dropped_messages_total[(bot_id, stream, participant_uuid)]

    def _ensure_started(self, client: BotWebsocketClient):
        if not client.started():
            logger.info("Starting websocket client for %s...", ", ".join(self._url_to_purposes[client.websocket_url]))
            client.start()

    def send_mixed_audio(self, payload: dict):
        if not self._mixed_audio_client:
            return
        self._ensure_started(self._mixed_audio_client)
        self._mixed_audio_client.send_async(payload)

    def send_per_participant_audio(self, payload: dict):
        if not self._per_participant_audio_client:
            return
        self._ensure_started(self._per_participant_audio_client)
        self._per_participant_audio_client.send_async(payload)

    def send_per_participant_video(self, payload: dict):
        if not self._per_participant_video_client:
            return
        self._ensure_started(self._per_participant_video_client)
        self._per_participant_video_client.send_async(payload)

    def cleanup(self):
        for client in self._clients:
            client.cleanup()
