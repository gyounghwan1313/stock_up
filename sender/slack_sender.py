import os
import json
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class SlackSender:
    def __init__(self, webhook_url: Optional[str] = None, bot_token: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        self.bot_token = bot_token or os.getenv('SLACK_BOT_TOKEN')
        
        if not self.webhook_url and not self.bot_token:
            raise ValueError("Either webhook_url or bot_token must be provided")
    
    def send_webhook_message(self, message: str, channel: Optional[str] = None, 
                           username: Optional[str] = None, icon_emoji: Optional[str] = None) -> bool:
        if not self.webhook_url:
            raise ValueError("Webhook URL not configured")
        
        payload = {
            "text": message
        }
        
        if channel:
            payload["channel"] = channel
        if username:
            payload["username"] = username
        if icon_emoji:
            payload["icon_emoji"] = icon_emoji
        
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to send webhook message: {e}")
            return False
    
    def send_bot_message(self, channel: str, message: str, 
                        thread_ts: Optional[str] = None, 
                        blocks: Optional[list] = None) -> Dict[str, Any]:
        if not self.bot_token:
            raise ValueError("Bot token not configured")
        
        url = "https://slack.com/api/chat.postMessage"
        headers = {
            "Authorization": f"Bearer {self.bot_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "channel": channel,
            "text": message
        }
        
        if thread_ts:
            payload["thread_ts"] = thread_ts
        if blocks:
            payload["blocks"] = blocks
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to send bot message: {e}")
            return {"ok": False, "error": str(e)}
    
    
    def send_formatted_message(self, title: str, fields: Dict[str, str], 
                             color: str = "good", channel: Optional[str] = None) -> bool:
        if not self.webhook_url:
            raise ValueError("Formatted messages require webhook URL")
        
        attachment = {
            "color": color,
            "title": title,
            "fields": [
                {
                    "title": key,
                    "value": value,
                    "short": True
                } for key, value in fields.items()
            ]
        }
        
        payload = {
            "attachments": [attachment]
        }
        
        if channel:
            payload["channel"] = channel
        
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to send formatted message: {e}")
            return False