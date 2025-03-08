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
        
        # Store last processed timestamps for each channel
        self.last_processed_timestamps: Dict[str, int] = {}
        self._load_last_processed_timestamps()
    
    def _load_last_processed_timestamps(self) -> None:
        """Load last processed message timestamps from Supabase storage"""
        for channel in self.config.telegram_channels:
            try:
                # Try to load timestamp from Supabase
                timestamp = self.storage.get_last_processed_timestamp("telegram", channel)
                if timestamp is not None:
                    self.last_processed_timestamps[channel] = timestamp
                    logger.info(f"Loaded last processed timestamp for channel {channel}: {timestamp} ({datetime.fromtimestamp(timestamp)})")
                else:
                    # Fall back to message_id for backward compatibility
                    message_id = self.storage.get_last_processed_message_id("telegram", channel)
                    if message_id:
                        logger.info(f"No timestamp found, using message ID for channel {channel}: {message_id}")
                        # We don't have a timestamp, so we'll set it to 0 and rely on message_id for this run
                        self.last_processed_timestamps[channel] = 0
                    else:
                        logger.info(f"No last processed data found for channel {channel}")
                        self.last_processed_timestamps[channel] = 0
            except Exception as e:
                logger.error(f"Error loading last processed data for channel {channel}: {e}")
                self.last_processed_timestamps[channel] = 0
    
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
        receive_limit = 60  # You can adjust this limit
        stats_data = {}
        
        # Get the last processed timestamp for this channel
        last_timestamp = self.get_last_processed_timestamp(channel)
        
        # We still need a message_id to start fetching from
        # For now, we'll use 0 to get the most recent messages
        from_message_id = 0
        
        # Track the highest timestamp we've seen in this batch
        highest_timestamp_seen = last_timestamp

        while receive:
            try:
                response = self.client.get_chat_history(
                    chat_id=channel,
                    limit=10,  # Fetch in smaller batches
                    from_message_id=from_message_id
                )
                response.wait()

                if not response.update or not response.update["messages"]:
                    logger.info(f"No more messages to fetch from {channel}")
                    break

                # Check if we're getting new messages
                new_messages_found = False
                
                for message in response.update["messages"]:
                    # Skip messages that are older than our last processed timestamp
                    message_timestamp = message.get("date", 0)
                    if message_timestamp <= last_timestamp:
                        logger.debug(f"Skipping message {message['id']} with timestamp {message_timestamp} (older than {last_timestamp})")
                        continue
                    
                    if message["content"]["@type"] == "messageText":
                        # Only process if this is a new message
                        if message["id"] not in stats_data:
                            new_messages_found = True
                            
                            # Store message text in stats_data
                            stats_data[message["id"]] = message["content"]["text"]["text"]
                            
                            # Track the highest timestamp we've seen
                            if message_timestamp > highest_timestamp_seen:
                                highest_timestamp_seen = message_timestamp
                                logger.debug(f"New highest timestamp: {highest_timestamp_seen} ({datetime.fromtimestamp(highest_timestamp_seen)})")
                            
                            message_link = self.client.call_method('getMessageLink', params={'chat_id': channel, 'message_id': message["id"]})
                            message_link.wait()

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
                                    'timestamp': message_timestamp,
                                    'reply_to_message_id': message.get('reply_to_message_id', None),
                                    'source_url': message_link.update['link']
                                }
                            )
                            messages.append(raw_msg)

                            # Update last processed timestamp
                            self.set_last_processed_timestamp(channel, message_timestamp)
                
                # If we didn't find any new messages, break the loop
                if not new_messages_found:
                    logger.info(f"No new messages found in batch from {channel}")
                    break
                
                # Get the last message ID to use as the starting point for the next batch
                if messages:
                    last_message = messages[-1]
                    from_message_id = int(last_message.metadata['message_id'])
                
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
    
    
        
    def get_last_processed_timestamp(self, channel: str = None) -> int:
        """Get the timestamp of the last processed message for a channel"""
        if channel is None:
            # If no channel is specified, return the oldest timestamp
            if not self.last_processed_timestamps:
                return 0
            return min(self.last_processed_timestamps.values())
        
        return self.last_processed_timestamps.get(channel, 0)
    
    def set_last_processed_timestamp(self, channel: str, timestamp: int) -> None:
        """Set the timestamp of the last processed message for a channel and persist to Supabase"""
        # Only update if this timestamp is newer than what we have
        current_timestamp = self.last_processed_timestamps.get(channel, 0)
        if timestamp <= current_timestamp:
            logger.debug(f"Not updating timestamp for channel {channel} as {timestamp} is not newer than {current_timestamp}")
            return
            
        # Update in-memory cache
        self.last_processed_timestamps[channel] = timestamp
        
        # Persist to Supabase
        try:
            success = self.storage.store_last_processed_timestamp("telegram", channel, timestamp)
            if success:
                logger.debug(f"Persisted last processed timestamp for channel {channel}: {timestamp} ({datetime.fromtimestamp(timestamp)})")
            else:
                logger.warning(f"Failed to persist last processed timestamp for channel {channel}")
        except Exception as e:
            logger.error(f"Error persisting last processed timestamp for channel {channel}: {e}")
    
    # Implement the abstract methods from BaseSource
    def get_last_processed_id(self, channel: str = None) -> str:
        """
        Get the ID of the last processed message (for backward compatibility)
        
        This method is maintained for compatibility with the BaseSource interface,
        but we're now using timestamps instead of message IDs.
        """
        # We don't have message IDs anymore, so return empty string
        return ""
    
    def set_last_processed_id(self, channel: str, message_id: str) -> None:
        """
        Set the ID of the last processed message (for backward compatibility)
        
        This method is maintained for compatibility with the BaseSource interface,
        but we're now using timestamps instead of message IDs.
        """
        # We're not using message IDs anymore, so this is a no-op
        logger.debug(f"set_last_processed_id called with {channel}, {message_id} - using timestamps instead")
    
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'client'):
            self.client.stop()
