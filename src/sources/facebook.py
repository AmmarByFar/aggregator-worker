import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
import facebook
from src.config import Config
from src.models import RawMessage
from src.sources.base import BaseSource

class FacebookSource(BaseSource):
    """Source for collecting messages from Facebook pages"""
    
    def __init__(self, config: Config):
        super().__init__(config)
        
        # Validate configuration
        if not config.facebook_access_token:
            raise ValueError("Facebook access token is required")
        
        if not config.facebook_pages:
            logger.warning("No Facebook pages configured")
        
        # Initialize Facebook client
        self.client = facebook.GraphAPI(access_token=config.facebook_access_token, version="3.1")
        
        # Store last processed post IDs for each page
        self.last_processed_ids: Dict[str, str] = {}
        self._load_last_processed_ids()
    
    def _load_last_processed_ids(self) -> None:
        """Load last processed post IDs from storage"""
        # In a real implementation, this would load from a persistent storage
        # For now, we'll just initialize with empty values
        for page in self.config.facebook_pages:
            self.last_processed_ids[page] = ""
    
    def collect_messages(self) -> List[RawMessage]:
        """Collect posts from configured Facebook pages"""
        all_messages: List[RawMessage] = []
        
        for page in self.config.facebook_pages:
            try:
                messages = self._collect_from_page(page)
                all_messages.extend(messages)
            except Exception as e:
                logger.error(f"Error collecting posts from Facebook page {page}: {e}")
        
        return all_messages
    
    def _collect_from_page(self, page: str) -> List[RawMessage]:
        """Collect posts from a specific Facebook page"""
        messages: List[RawMessage] = []
        
        # Get the last processed post ID for this page
        last_id = self.get_last_processed_id(page)
        
        # In a real implementation, we would use the Facebook API to get posts
        # For now, we'll just return an empty list
        
        # Example of how this would work with the Facebook API:
        """
        # Get page posts
        posts = self.client.get_connections(
            id=page,
            connection_name='posts',
            fields='id,message,created_time,from,permalink_url,shares,reactions.summary(true)'
        )
        
        for post in posts['data']:
            # Skip posts without a message
            if 'message' not in post:
                continue
                
            # Skip posts that have already been processed
            if last_id and post['id'] <= last_id:
                continue
                
            message = RawMessage(
                source="facebook",
                source_id=post['id'],
                content=post['message'],
                author=post.get('from', {}).get('name', None),
                timestamp=datetime.strptime(post['created_time'], '%Y-%m-%dT%H:%M:%S+0000'),
                metadata={
                    'page': page,
                    'post_id': post['id'],
                    'permalink_url': post.get('permalink_url', None),
                    'shares': post.get('shares', {}).get('count', 0),
                    'reactions': post.get('reactions', {}).get('summary', {}).get('total_count', 0),
                }
            )
            messages.append(message)
            
            # Update last processed ID
            if not last_id or post['id'] > last_id:
                self.set_last_processed_id(page, post['id'])
        """
        
        logger.info(f"Collected {len(messages)} posts from Facebook page {page}")
        return messages
    
    def get_last_processed_id(self, page: str = None) -> str:
        """Get the ID of the last processed post for a page"""
        if page is None:
            # If no page is specified, return the oldest ID
            if not self.last_processed_ids:
                return ""
            return min(self.last_processed_ids.values())
        
        return self.last_processed_ids.get(page, "")
    
    def set_last_processed_id(self, page: str, post_id: str) -> None:
        """Set the ID of the last processed post for a page"""
        self.last_processed_ids[page] = post_id
        # In a real implementation, this would persist to storage
