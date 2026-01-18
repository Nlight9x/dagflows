import httpx
from typing import Union, Optional


class Message:
    """Base class for different message types to be sent via alert connectors"""

    def get_type(self) -> str:
        """
        Return the message type (e.g., 'text', 'photo', 'document', 'location').
        Subclasses should override this method.
        """
        return 'text'

    def to_text(self) -> str:
        """
        Convert message to plain text format.
        Subclasses should override this method if they can be converted to text.
        """
        return ""

    def to_payload(self) -> dict:
        """
        Convert message to payload dictionary for Telegram API.
        Subclasses should override this method to return the appropriate payload.
        
        Returns:
            dict: Payload dictionary with required fields for the message type
        """
        return {}


class AlertConnector:
    def __init__(self):
        pass

    def send(self, message):
        pass


class TelegramAlert(AlertConnector):
    _api_base_url = "https://api.telegram.org/bot"
    
    # Map message types to Telegram API endpoints
    _message_type_endpoints = {
        'text': 'sendMessage',
        'photo': 'sendPhoto',
        'document': 'sendDocument',
        'location': 'sendLocation',
        'poll': 'sendPoll',
        'animation': 'sendAnimation',
        'video': 'sendVideo',
        'audio': 'sendAudio',
        'voice': 'sendVoice',
    }
    
    def __init__(self, bot_token: str, chat_id: Union[str, int], timeout: float = 30.0, parse_mode: Optional[str] = None):
        """
        Initialize Telegram Alert connector.
        
        Args:
            bot_token: Telegram Bot Token (get from @BotFather)
            chat_id: Telegram Chat ID (user ID or channel ID where to send messages)
            timeout: Request timeout in seconds (default: 30.0)
            parse_mode: Message parsing mode - 'HTML', 'Markdown', or 'MarkdownV2' (optional)
        """
        super().__init__()
        if not bot_token:
            raise ValueError("bot_token is required")
        if not chat_id:
            raise ValueError("chat_id is required")
        
        self._bot_token = bot_token
        self._chat_id = str(chat_id)
        self._timeout = timeout
        self._parse_mode = parse_mode
        self._base_url = f"{self._api_base_url}{bot_token}/"

    def send_message(self, message: Union[str, Message], disable_notification: bool = False, disable_web_page_preview: bool = True, **kwargs):
        """
        Send a message to Telegram chat. Supports multiple message types via Message objects.
        
        Args:
            message: Message to send - can be:
                    - str: Plain text message (converted to text message)
                    - Message object: Any Message subclass with get_type() and to_payload() methods
            disable_notification: If True, send silently (default: False)
            disable_web_page_preview: If True, disable link previews for text messages (default: True)
            **kwargs: Additional parameters passed to message.to_payload() if message is a Message object
        
        Returns:
            dict: Response from Telegram API containing message details
        
        Raises:
            ValueError: If message is empty or invalid
            Exception: If API request fails
        """
        # Handle string messages (convert to text message)
        if isinstance(message, str):
            text = message
            if not text or not text.strip():
                raise ValueError("Message cannot be empty")
            
            payload = {
                "chat_id": self._chat_id,
                "text": text,
                "disable_notification": disable_notification,
                "disable_web_page_preview": disable_web_page_preview
            }
            
            if self._parse_mode:
                payload["parse_mode"] = self._parse_mode
            
            endpoint = self._message_type_endpoints['text']
        
        # Handle Message objects
        elif isinstance(message, Message):
            msg_type = message.get_type()
            if msg_type not in self._message_type_endpoints:
                raise ValueError(f"Unsupported message type: {msg_type}")
            
            # Get payload from message object
            payload = message.to_payload()
            if not payload:
                raise ValueError("Message.to_payload() returned empty dictionary")
            
            # Merge common fields
            payload.setdefault("chat_id", self._chat_id)
            payload.setdefault("disable_notification", disable_notification)
            
            # Apply parse_mode for text-based messages if specified
            if msg_type in ['text', 'photo', 'document'] and self._parse_mode and 'parse_mode' not in payload:
                payload["parse_mode"] = self._parse_mode
            
            # Apply disable_web_page_preview for text messages
            if msg_type == 'text' and 'disable_web_page_preview' not in payload:
                payload["disable_web_page_preview"] = disable_web_page_preview
            
            # Merge additional kwargs
            payload.update(kwargs)
            
            endpoint = self._message_type_endpoints[msg_type]
        
        else:
            raise ValueError(f"Unsupported message type: {type(message)}")
        
        # Send request to appropriate endpoint
        api_url = f"{self._base_url}{endpoint}"
        
        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(api_url, json=payload)
                response.raise_for_status()
                result = response.json()
                
                if not result.get("ok"):
                    error_description = result.get("description", "Unknown error")
                    raise Exception(f"Telegram API error: {error_description}")
                
                return result.get("result", {})
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error when sending Telegram message: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise Exception(f"Request error when sending Telegram message: {e}")
        except Exception as e:
            raise Exception(f"Failed to send Telegram message: {e}")


# Example: Custom Message classes
class TextMessage(Message):
    """Text message with optional formatting"""
    
    def __init__(self, text: str, parse_mode: Optional[str] = None):
        self.text = text
        self.parse_mode = parse_mode
    
    def get_type(self) -> str:
        return 'text'
    
    def to_text(self) -> str:
        return self.text
    
    def to_payload(self) -> dict:
        payload = {'text': self.text}
        if self.parse_mode:
            payload['parse_mode'] = self.parse_mode
        return payload


class PhotoMessage(Message):
    """Photo message with URL"""
    
    def __init__(self, photo_url: str, caption: str = ""):
        self.photo_url = photo_url
        self.caption = caption
    
    def get_type(self) -> str:
        return 'photo'
    
    def to_text(self) -> str:
        return self.caption if self.caption else f"Photo: {self.photo_url}"
    
    def to_payload(self) -> dict:
        payload = {'photo': self.photo_url}
        if self.caption:
            payload['caption'] = self.caption
        return payload


# Test code
if __name__ == "__main__":
    import os
    
    # Get credentials from environment variables or set them here
    # Example: export TELEGRAM_BOT_TOKEN="your_bot_token"
    #          export TELEGRAM_CHAT_ID="your_chat_id"
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "8485577094:AAH6BhSCe9928Qs1fZbPAw3IgH3Mjc-Bauc")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "285755725")
    
    if not bot_token or not chat_id:
        print("⚠️  Warning: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables not set.")
        print("   Set them to test sending messages:")
        print("   export TELEGRAM_BOT_TOKEN='your_bot_token'")
        print("   export TELEGRAM_CHAT_ID='your_chat_id'")
        print("\n   Or uncomment and set the values below:")
        # bot_token = "your_bot_token_here"
        # chat_id = "your_chat_id_here"
    else:
        try:
            # Initialize Telegram Alert
            telegram = TelegramAlert(
                bot_token=bot_token,
                chat_id=chat_id,
                parse_mode="HTML"  # Optional: 'HTML', 'Markdown', or 'MarkdownV2'
            )
            
            # Test 1: Send plain text message (string)
            print("Test 1: Sending plain text message...")
            result = telegram.send_message("Hello! This is a test message from TelegramAlert 🚀")
            print(f"✓ Message sent successfully! Message ID: {result.get('message_id')}")
            
            # Test 2: Send text message using TextMessage class
            print("\nTest 2: Sending formatted text message...")
            text_msg = TextMessage(
                text="<b>Bold text</b> and <i>italic text</i> with <code>code</code>",
                parse_mode="HTML"
            )
            result = telegram.send_message(text_msg)
            print(f"✓ Formatted message sent! Message ID: {result.get('message_id')}")
            
            # Test 3: Send photo message (uncomment to test if you have a photo URL)
            # print("\nTest 3: Sending photo message...")
            # photo_msg = PhotoMessage(
            #     photo_url="https://picsum.photos/200/300",
            #     caption="Random test image"
            # )
            # result = telegram.send_message(photo_msg)
            # print(f"✓ Photo sent! Message ID: {result.get('message_id')}")
            
            print("\n✅ All tests completed successfully!")
            
        except Exception as e:
            print(f"❌ Error testing Telegram Alert: {e}")
