from typing import List, Dict, Any
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
            supabase_key=config.supabase_key
        )
        
        # Table name for news items
        self.table_name = "news_items"
    
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
        response = self.client.table(self.table_name) \
            .select("id") \
            .eq("source", news_item.source) \
            .eq("source_id", news_item.source_id) \
            .execute()
        
        if response.data:
            logger.debug(f"News item {news_item.source_id} from {news_item.source} already exists")
            return
        
        # Convert news item to dict for storage
        item_dict = news_item.dict(exclude={"id"})
        
        # Insert news item
        response = self.client.table(self.table_name) \
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
        response = self.client.table(self.table_name) \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
        
        return response.data if response.data else []
