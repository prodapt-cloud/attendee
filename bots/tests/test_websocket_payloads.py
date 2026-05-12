import unittest

from bots.websocket_payloads import mixed_audio_websocket_payload, per_participant_audio_websocket_payload


class TestWebsocketPayloads(unittest.TestCase):
    def test_mixed_audio_payload_includes_sequence_when_provided(self):
        payload = mixed_audio_websocket_payload(
            chunk=b"\x00\x00\x01\x00",
            input_sample_rate=16000,
            output_sample_rate=16000,
            bot_object_id="bot_123",
            sequence=42,
        )

        self.assertEqual(payload["data"]["sequence"], 42)

    def test_mixed_audio_payload_omits_sequence_when_not_provided(self):
        payload = mixed_audio_websocket_payload(
            chunk=b"\x00\x00\x01\x00",
            input_sample_rate=16000,
            output_sample_rate=16000,
            bot_object_id="bot_123",
        )

        self.assertNotIn("sequence", payload["data"])

    def test_per_participant_audio_payload_includes_sequence_when_provided(self):
        payload = per_participant_audio_websocket_payload(
            participant_uuid="participant_123",
            chunk=b"\x00\x00\x01\x00",
            input_sample_rate=16000,
            output_sample_rate=16000,
            bot_object_id="bot_123",
            sequence=7,
        )

        self.assertEqual(payload["data"]["sequence"], 7)
        self.assertEqual(payload["data"]["participant_uuid"], "participant_123")


if __name__ == "__main__":
    unittest.main()
