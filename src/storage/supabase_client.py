from typing import List, Dict, Any, Optional
from loguru import logger
from supabase import create_client, Client

from src.config import Config
from src.models import NewsItem

class SupabaseClient:
    """Client for storing news items in Supabase"""
    
    def __init__(self, config: Config):
        self.config = config
        
        # Validate configuration
        if not config.supabase_url or not config.supabase_key:
            raise ValueError("Supabase URL and key are required")
        
        # Initialize Supabase client
        self.client: Client = create_client(
            supabase_url=config.supabase_url,
            supabase_key=config.supabase_key,
        )
        
        # Table names
        self.news_items_table = "news_items"
        self.telegram_last_processed_table = "telegram_last_processed"
    
    def store_news_items(self, news_items: List[NewsItem]) -> None:
        """
        Store news items in Supabase
        
        Args:
            news_items (List[NewsItem]): List of news items to store
        """
        if not news_items:
            logger.warning("No news items to store")
            return
        
        for item in news_items:
            try:
                self._store_news_item(item)
            except Exception as e:
                logger.error(f"Error storing news item {item.source_id} from {item.source}: {e}")
    
    def _store_news_item(self, news_item: NewsItem) -> None:
        """
        Store a single news item in Supabase
        
        Args:
            news_item (NewsItem): News item to store
        """
        # Check if the news item already exists
        response = self.client.table(self.news_items_table) \
            .select("id") \
            .eq("source", news_item.source) \
            .eq("source_id", news_item.source_id) \
            .execute()
        
        if response.data:
            logger.debug(f"News item {news_item.source_id} from {news_item.source} already exists")
            return
        
        # Convert news item to dict for storage with proper datetime serialization
        # First convert to JSON string with datetime handling, then parse back to dict
        item_dict = news_item.dict(exclude={"id"})
        
        # Convert datetime objects to ISO format strings
        if "timestamp" in item_dict and item_dict["timestamp"]:
            item_dict["timestamp"] = item_dict["timestamp"].isoformat()
        if "created_at" in item_dict and item_dict["created_at"]:
            item_dict["created_at"] = item_dict["created_at"].isoformat()
        
        # Insert news item
        response = self.client.table(self.news_items_table) \
            .insert(item_dict) \
            .execute()
        
        if response.data:
            logger.info(f"Stored news item {news_item.title} from {news_item.source}")
            # Update news item with ID from Supabase
            news_item.id = response.data[0]["id"]
        else:
            logger.error(f"Failed to store news item {news_item.title} from {news_item.source}")
    
    def get_news_items(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get news items from Supabase
        
        Args:
            limit (int, optional): Maximum number of items to retrieve. Defaults to 100.
            
        Returns:
            List[Dict[str, Any]]: List of news items
        """
        response = self.client.table(self.news_items_table) \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        
        return response.data if response.data else []
    
    def get_last_processed_message_id(self, source: str, channel: str) -> Optional[str]:
        """
        Get the last processed message ID for a specific source and channel
        
        Args:
            source (str): Source name (e.g., 'telegram')
            channel (str): Channel identifier
            
        Returns:
            Optional[str]: Last processed message ID or None if not found
        """
        try:
            response = self.client.table(self.telegram_last_processed_table) \
                .select("message_id") \
                .eq("source", source) \
                .eq("channel", channel) \
                .execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]["message_id"]
            return None
        except Exception as e:
            logger.error(f"Error retrieving last processed ID for {source}/{channel}: {e}")
            return None
    
    def get_last_processed_timestamp(self, source: str, channel: str) -> Optional[int]:
        """
        Get the last processed message timestamp for a specific source and channel
        
        Args:
            source (str): Source name (e.g., 'telegram')
            channel (str): Channel identifier
            
        Returns:
            Optional[int]: Last processed timestamp (Unix time) or None if not found
        """
        try:
            response = self.client.table(self.telegram_last_processed_table) \
                .select("timestamp") \
                .eq("source", source) \
                .eq("channel", channel) \
                .execute()
            
            if response.data and len(response.data) > 0 and response.data[0].get("timestamp") is not None:
                return response.data[0]["timestamp"]
            return None
        except Exception as e:
            logger.error(f"Error retrieving last processed timestamp for {source}/{channel}: {e}")
            return None
    
    def store_last_processed_message_id(self, source: str, channel: str, message_id: str) -> bool:
        """
        Store the last processed message ID for a specific source and channel
        
        Args:
            source (str): Source name (e.g., 'telegram')
            channel (str): Channel identifier
            message_id (str): Last processed message ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if record exists
            response = self.client.table(self.telegram_last_processed_table) \
                .select("id") \
                .eq("source", source) \
                .eq("channel", channel) \
                .execute()
            
            data = {
                "source": source,
                "channel": channel,
                "message_id": message_id,
                "updated_at": "now()"
            }
            
            if response.data and len(response.data) > 0:
                # Update existing record
                record_id = response.data[0]["id"]
                response = self.client.table(self.telegram_last_processed_table) \
                    .update(data) \
                    .eq("id", record_id) \
                    .execute()
            else:
                # Insert new record
                response = self.client.table(self.telegram_last_processed_table) \
                    .insert(data) \
                    .execute()
            
            return bool(response.data)
        except Exception as e:
            logger.error(f"Error storing last processed ID for {source}/{channel}: {e}")
            return False
    
    def store_last_processed_timestamp(self, source: str, channel: str, timestamp: int) -> bool:
        """
        Store the last processed message timestamp for a specific source and channel
        
        Args:
            source (str): Source name (e.g., 'telegram')
            channel (str): Channel identifier
            timestamp (int): Last processed message timestamp (Unix time)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if record exists
            response = self.client.table(self.telegram_last_processed_table) \
                .select("id") \
                .eq("source", source) \
                .eq("channel", channel) \
                .execute()
            
            data = {
                "source": source,
                "channel": channel,
                "timestamp": timestamp,
                "updated_at": "now()"
            }
            
            if response.data and len(response.data) > 0:
                # Update existing record
                record_id = response.data[0]["id"]
                response = self.client.table(self.telegram_last_processed_table) \
                    .update(data) \
                    .eq("id", record_id) \
                    .execute()
            else:
                # Insert new record
                response = self.client.table(self.telegram_last_processed_table) \
                    .insert(data) \
                    .execute()
            
            return bool(response.data)
        except Exception as e:
            logger.error(f"Error storing last processed timestamp for {source}/{channel}: {e}")
            return False
