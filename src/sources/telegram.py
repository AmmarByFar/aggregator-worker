import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
from telegram.client import Telegram
from src.config import Config
from src.models import RawMessage
from src.sources.base import BaseSource
from src.storage.supabase_client import SupabaseClient

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
        
        # Initialize Supabase client for persistence
        self.storage = SupabaseClient(config)
        
        # Store last processed message IDs for each channel
        self.last_processed_ids: Dict[str, str] = {}
        self._load_last_processed_ids()
    
    def _load_last_processed_ids(self) -> None:
        """Load last processed message IDs from Supabase storage"""
        for channel in self.config.telegram_channels:
            try:
                # Try to load from Supabase
                message_id = self.storage.get_last_processed_message_id("telegram", channel)
                self.last_processed_ids[channel] = message_id if message_id else ""
                if message_id:
                    logger.info(f"Loaded last processed message ID for channel {channel}: {message_id}")
                else:
                    logger.info(f"No last processed message ID found for channel {channel}")
            except Exception as e:
                logger.error(f"Error loading last processed ID for channel {channel}: {e}")
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
        receive = True
        receive_limit = 2  # You can adjust this limit
        stats_data = {}
        
        # Get the last processed message ID for this channel
        from_message_id = self.get_last_processed_id(channel)
        if not from_message_id:
            from_message_id = 0
        else:
            from_message_id = int(from_message_id)
        
        # Track the last message ID we've seen in this batch
        last_seen_message_id = from_message_id

        while receive:
            try:
                response = self.client.get_chat_history(
                    chat_id=channel,
                    limit=2,  # Fetch in smaller batches
                    from_message_id=from_message_id
                )
                response.wait()

                if not response.update or not response.update["messages"]:
                    logger.info(f"No more messages to fetch from {channel}")
                    break

                # Check if we're getting new messages
                new_messages_found = False
                
                for message in response.update["messages"]:
                    if message["content"]["@type"] == "messageText":
                        # Only process if this is a new message
                        if message["id"] not in stats_data:
                            new_messages_found = True
                            
                            # Store message text in stats_data
                            stats_data[message["id"]] = message["content"]["text"]["text"]
                            
                            # Track the last message ID we've seen
                            if message["id"] > last_seen_message_id:
                                last_seen_message_id = message["id"]

                            # Create RawMessage object
                            raw_msg = RawMessage(
                                source="telegram",
                                source_id=str(message["id"]),
                                content=message["content"]["text"]["text"],
                                author=message.get("sender_user_id", None),
                                timestamp=datetime.fromtimestamp(message["date"]),
                                metadata={
                                    'channel': channel,
                                    'message_id': message["id"],
                                    'reply_to_message_id': message.get('reply_to_message_id', None),
                                }
                            )
                            messages.append(raw_msg)

                            # Update last processed ID
                            self.set_last_processed_id(channel, str(message["id"]))
                
                # If we didn't find any new messages, break the loop
                if not new_messages_found:
                    logger.info(f"No new messages found in batch from {channel}")
                    break
                
                # Update from_message_id to fetch the next batch of messages
                from_message_id = last_seen_message_id
                
                total_messages = len(stats_data)
                logger.info(f"[{total_messages}/{receive_limit}] messages received from {channel}")
                
                # Check if we've reached the limit
                if total_messages >= receive_limit:
                    logger.info(f"Reached message limit ({receive_limit}) for {channel}")
                    receive = False

            except Exception as e:
                logger.error(f"Error collecting messages from Telegram channel {channel}: {e}")
                break

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
        """Set the ID of the last processed message for a channel and persist to Supabase"""
        # Update in-memory cache
        self.last_processed_ids[channel] = message_id
        
        # Persist to Supabase
        try:
            success = self.storage.store_last_processed_message_id("telegram", channel, message_id)
            if success:
                logger.debug(f"Persisted last processed message ID for channel {channel}: {message_id}")
            else:
                logger.warning(f"Failed to persist last processed message ID for channel {channel}")
        except Exception as e:
            logger.error(f"Error persisting last processed ID for channel {channel}: {e}")
    
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'client'):
            self.client.stop()
