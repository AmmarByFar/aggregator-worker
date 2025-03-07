import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
from telegram.client import Telegram
from src.config import Config
from src.models import RawMessage
from src.sources.base import BaseSource

class TelegramSource(BaseSource):
    """Source for collecting messages from Telegram channels"""
    
    def __init__(self, config: Config):
        super().__init__(config)
        
        # Validate configuration
        if not config.telegram_api_id or not config.telegram_api_hash:
            raise ValueError("Telegram API ID and hash are required")
        
        if not config.telegram_channels:
            logger.warning("No Telegram channels configured")
        
        # Initialize Telegram client
        self.client = Telegram(
            api_id=config.telegram_api_id,
            api_hash=config.telegram_api_hash,
            phone=config.telegram_phone,
            database_encryption_key='changeme1234',
            files_directory='/tmp/.tdlib_files/'
        )
        
        # Start the client
        self.client.login()
        
        # Store last processed message IDs for each channel
        self.last_processed_ids: Dict[str, str] = {}
        self._load_last_processed_ids()
    
    def _load_last_processed_ids(self) -> None:
        """Load last processed message IDs from storage"""
        # In a real implementation, this would load from a persistent storage
        # For now, we'll just initialize with empty values
        for channel in self.config.telegram_channels:
            self.last_processed_ids[channel] = ""
    
    def collect_messages(self) -> List[RawMessage]:
        """Collect messages from configured Telegram channels"""
        all_messages: List[RawMessage] = []
        
        for channel in self.config.telegram_channels:
            try:
                messages = self._collect_from_channel(channel)
                all_messages.extend(messages)
            except Exception as e:
                logger.error(f"Error collecting messages from Telegram channel {channel}: {e}")
        
        return all_messages
    
    def _collect_from_channel(self, channel: str) -> List[RawMessage]:
        """Collect messages from a specific Telegram channel"""
        messages: List[RawMessage] = []
        
        # Get the last processed message ID for this channel
        last_id = self.get_last_processed_id(channel)


        params = {
            'chat_list_': None,  # or {} for main chat list
            'limit_': 100
        }
        method_call = self.client.call_method('loadChats', params)
        logger.info(f"method_call: {method_call.ok_received}")


        result = self.client.get_chat_history(
            chat_id=channel,
            limit=100,
            from_message_id=last_id if last_id else 0
        )
        
        if result and result.update:
            for msg in result.update['messages']:
                if 'content' in msg and 'text' in msg['content']:
                    message = RawMessage(
                        source="telegram",
                        source_id=str(msg['id']),
                        content=msg['content']['text']['text'],
                        author=msg.get('sender_user_id', None),
                        timestamp=datetime.fromtimestamp(msg['date']),
                        metadata={
                            'channel': channel,
                            'message_id': msg['id'],
                            'reply_to_message_id': msg.get('reply_to_message_id', None),
                        }
                    )
                    messages.append(message)
                    
                    # Update last processed ID
                    if not last_id or int(msg['id']) > int(last_id):
                        self.set_last_processed_id(channel, str(msg['id']))     
        
        logger.info(f"Collected {len(messages)} messages from Telegram channel {channel}")
        return messages
    
    
        
    def get_last_processed_id(self, channel: str = None) -> str:
        """Get the ID of the last processed message for a channel"""
        if channel is None:
            # If no channel is specified, return the oldest ID
            if not self.last_processed_ids:
                return ""
            return min(self.last_processed_ids.values())
        
        return self.last_processed_ids.get(channel, "")
    
    def set_last_processed_id(self, channel: str, message_id: str) -> None:
        """Set the ID of the last processed message for a channel"""
        self.last_processed_ids[channel] = message_id
        # In a real implementation, this would persist to storage
    
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'client'):
            self.client.stop()
