import json
from unittest.mock import patch

import pytest
import responses

from sender import SlackSender


class TestSlackSender:
    @patch.dict("os.environ", {}, clear=True)
    def test_init_with_webhook_url(self):
        sender = SlackSender(webhook_url="https://hooks.slack.com/test")
        assert sender.webhook_url == "https://hooks.slack.com/test"
        assert sender.bot_token is None

    @patch.dict("os.environ", {}, clear=True)
    def test_init_with_bot_token(self):
        sender = SlackSender(bot_token="xoxb-test-token")
        assert sender.bot_token == "xoxb-test-token"
        assert sender.webhook_url is None

    @patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "https://env.webhook.url"})
    def test_init_with_env_webhook(self):
        sender = SlackSender()
        assert sender.webhook_url == "https://env.webhook.url"

    @patch.dict("os.environ", {"SLACK_BOT_TOKEN": "xoxb-env-token"})
    def test_init_with_env_bot_token(self):
        sender = SlackSender()
        assert sender.bot_token == "xoxb-env-token"

    def test_init_without_credentials_raises_error(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(
                ValueError, match="Either webhook_url or bot_token must be provided"
            ):
                SlackSender()

    @responses.activate
    def test_send_webhook_message_success(self):
        webhook_url = "https://hooks.slack.com/test"
        responses.add(responses.POST, webhook_url, json={"ok": True}, status=200)

        sender = SlackSender(webhook_url=webhook_url)
        result = sender.send_webhook_message("Test message")

        assert result is True
        assert len(responses.calls) == 1

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["text"] == "Test message"

    @responses.activate
    def test_send_webhook_message_with_options(self):
        webhook_url = "https://hooks.slack.com/test"
        responses.add(responses.POST, webhook_url, json={"ok": True}, status=200)

        sender = SlackSender(webhook_url=webhook_url)
        result = sender.send_webhook_message(
            message="Test message",
            channel="#test",
            username="TestBot",
            icon_emoji=":robot_face:",
        )

        assert result is True
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["text"] == "Test message"
        assert request_body["channel"] == "#test"
        assert request_body["username"] == "TestBot"
        assert request_body["icon_emoji"] == ":robot_face:"

    @responses.activate
    def test_send_webhook_message_failure(self):
        webhook_url = "https://hooks.slack.com/test"
        responses.add(responses.POST, webhook_url, status=400)

        sender = SlackSender(webhook_url=webhook_url)
        result = sender.send_webhook_message("Test message")

        assert result is False

    @patch.dict("os.environ", {}, clear=True)
    def test_send_webhook_message_without_webhook_url(self):
        sender = SlackSender(bot_token="xoxb-test")

        with pytest.raises(ValueError, match="Webhook URL not configured"):
            sender.send_webhook_message("Test message")

    @responses.activate
    def test_send_bot_message_success(self):
        bot_url = "https://slack.com/api/chat.postMessage"
        responses.add(
            responses.POST,
            bot_url,
            json={"ok": True, "ts": "1234567890.123456"},
            status=200,
        )

        sender = SlackSender(bot_token="xoxb-test")
        result = sender.send_bot_message("#general", "Test message")

        assert result["ok"] is True
        assert "ts" in result

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["channel"] == "#general"
        assert request_body["text"] == "Test message"

    @responses.activate
    def test_send_bot_message_with_thread(self):
        bot_url = "https://slack.com/api/chat.postMessage"
        responses.add(responses.POST, bot_url, json={"ok": True}, status=200)

        sender = SlackSender(bot_token="xoxb-test")
        result = sender.send_bot_message(
            "#general", "Reply message", thread_ts="1234567890.123456"
        )

        assert result["ok"] is True
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body["thread_ts"] == "1234567890.123456"

    @patch.dict("os.environ", {}, clear=True)
    def test_send_bot_message_without_token(self):
        sender = SlackSender(webhook_url="https://test.webhook")

        with pytest.raises(ValueError, match="Bot token not configured"):
            sender.send_bot_message("#general", "Test message")

    @responses.activate
    def test_send_formatted_message_success(self):
        webhook_url = "https://hooks.slack.com/test"
        responses.add(responses.POST, webhook_url, json={"ok": True}, status=200)

        sender = SlackSender(webhook_url=webhook_url)
        fields = {"AAPL": "$150.25 (+1.69%)", "GOOGL": "$2,750.30 (-0.45%)"}
        result = sender.send_formatted_message("Stock Summary", fields, "good")

        assert result is True
        request_body = json.loads(responses.calls[0].request.body)

        attachment = request_body["attachments"][0]
        assert attachment["color"] == "good"
        assert attachment["title"] == "Stock Summary"
        assert len(attachment["fields"]) == 2
        assert attachment["fields"][0]["title"] == "AAPL"
        assert attachment["fields"][0]["value"] == "$150.25 (+1.69%)"

    @patch.dict("os.environ", {}, clear=True)
    def test_send_formatted_message_without_webhook(self):
        sender = SlackSender(bot_token="xoxb-test")

        with pytest.raises(ValueError, match="Formatted messages require webhook URL"):
            sender.send_formatted_message("Title", {"key": "value"})

    @responses.activate
    def test_network_timeout_handling(self):
        webhook_url = "https://hooks.slack.com/test"
        responses.add(
            responses.POST,
            webhook_url,
            body=responses.ConnectionError("Connection timeout"),
        )

        sender = SlackSender(webhook_url=webhook_url)
        result = sender.send_webhook_message("Test message")

        assert result is False
