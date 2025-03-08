import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
import tweepy
from src.config import Config
from src.models import RawMessage
from src.sources.base import BaseSource
from src.storage.supabase_client import SupabaseClient

class TwitterSource(BaseSource):
    """Source for collecting messages from Twitter/X timeline"""
    
    def __init__(self, config: Config):
        super().__init__(config)
        
        # Validate configuration
        if not config.twitter_api_key or not config.twitter_api_secret:
            raise ValueError("Twitter API key and secret are required")
        
        if not config.twitter_access_token or not config.twitter_access_secret:
            raise ValueError("Twitter access token and secret are required")
        
        if not config.twitter_accounts:
            logger.warning("No Twitter accounts configured")
        
        # Initialize Twitter client
        auth = tweepy.OAuth1UserHandler(
            consumer_key=config.twitter_api_key,
            consumer_secret=config.twitter_api_secret,
            access_token=config.twitter_access_token,
            access_token_secret=config.twitter_access_secret
        )
        self.client = tweepy.API(auth)
        
        # Initialize Supabase client for persistence
        self.storage = SupabaseClient(config)
        
        # Store last processed timestamps
        self.last_processed_timestamp: int = 0
        self._load_last_processed_timestamp()
    
    def _load_last_processed_timestamp(self) -> None:
        """Load last processed message timestamp from Supabase storage"""
        try:
            # Try to load timestamp from Supabase
            timestamp = self.storage.get_last_processed_timestamp("twitter", "timeline")
            if timestamp is not None:
                self.last_processed_timestamp = timestamp
                logger.info(f"Loaded last processed timestamp: {timestamp} ({datetime.fromtimestamp(timestamp)})")
            else:
                logger.info("No last processed timestamp found")
                self.last_processed_timestamp = 0
        except Exception as e:
            logger.error(f"Error loading last processed timestamp: {e}")
            self.last_processed_timestamp = 0
    
    def collect_messages(self) -> List[RawMessage]:
        """Collect tweets from home timeline"""
        messages: List[RawMessage] = []
        
        try:
            # Get the last processed timestamp
            last_timestamp = self.get_last_processed_timestamp()
            
            # Track the highest timestamp we've seen in this batch
            highest_timestamp_seen = last_timestamp
            
            # Get tweets from home timeline
            tweets = self.client.home_timeline(
                count=100,  # Maximum allowed by Twitter
                tweet_mode='extended',  # Get full tweet text
                include_rts=True  # Include retweets
            )
            
            for tweet in tweets:
                # Convert tweet created_at to timestamp
                tweet_timestamp = int(tweet.created_at.timestamp())
                
                # Skip tweets that are older than our last processed timestamp
                if tweet_timestamp <= last_timestamp:
                    logger.debug(f"Skipping tweet {tweet.id} with timestamp {tweet_timestamp} (older than {last_timestamp})")
                    continue
                
                # Use full_text for the complete tweet content
                content = tweet.full_text if hasattr(tweet, 'full_text') else tweet.text
                
                message = RawMessage(
                    source="twitter",
                    source_id=str(tweet.id),
                    content=content,
                    author=tweet.user.screen_name,
                    timestamp=tweet.created_at,
                    metadata={
                        'tweet_id': tweet.id,
                        'timestamp': tweet_timestamp,
                        'tweet_url': f"https://twitter.com/{tweet.user.screen_name}/status/{tweet.id}",
                        'retweet_count': tweet.retweet_count,
                        'favorite_count': tweet.favorite_count,
                        'is_retweet': hasattr(tweet, 'retweeted_status'),
                        'hashtags': [h['text'] for h in tweet.entities.get('hashtags', [])],
                        'urls': [u['expanded_url'] for u in tweet.entities.get('urls', [])],
                        'user_mentions': [m['screen_name'] for m in tweet.entities.get('user_mentions', [])],
                    }
                )
                messages.append(message)
                
                # Track the highest timestamp we've seen
                if tweet_timestamp > highest_timestamp_seen:
                    highest_timestamp_seen = tweet_timestamp
                    logger.debug(f"New highest timestamp: {highest_timestamp_seen} ({datetime.fromtimestamp(highest_timestamp_seen)})")
            
            # Update last processed timestamp if we saw any new tweets
            if highest_timestamp_seen > last_timestamp:
                self.set_last_processed_timestamp(highest_timestamp_seen)
        
            logger.info(f"Collected {len(messages)} tweets from timeline")
            
        except Exception as e:
            logger.error(f"Error collecting tweets from timeline: {e}")
        
        return messages
    
    def get_last_processed_timestamp(self) -> int:
        """Get the timestamp of the last processed tweet"""
        return self.last_processed_timestamp
    
    def set_last_processed_timestamp(self, timestamp: int) -> None:
        """Set the timestamp of the last processed tweet and persist to Supabase"""
        # Only update if this timestamp is newer than what we have
        if timestamp <= self.last_processed_timestamp:
            logger.debug(f"Not updating timestamp as {timestamp} is not newer than {self.last_processed_timestamp}")
            return
            
        # Update in-memory cache
        self.last_processed_timestamp = timestamp
        
        # Persist to Supabase
        try:
            success = self.storage.store_last_processed_timestamp("twitter", "timeline", timestamp)
            if success:
                logger.debug(f"Persisted last processed timestamp: {timestamp} ({datetime.fromtimestamp(timestamp)})")
            else:
                logger.warning("Failed to persist last processed timestamp")
        except Exception as e:
            logger.error(f"Error persisting last processed timestamp: {e}")
    
    # Implement the abstract methods from BaseSource
    def get_last_processed_id(self) -> str:
        """
        Get the ID of the last processed message (for backward compatibility)
        
        This method is maintained for compatibility with the BaseSource interface,
        but we're now using timestamps instead of message IDs.
        """
        # We don't have message IDs anymore, so return empty string
        return ""
    
    def set_last_processed_id(self, message_id: str) -> None:
        """
        Set the ID of the last processed message (for backward compatibility)
        
        This method is maintained for compatibility with the BaseSource interface,
        but we're now using timestamps instead of message IDs.
        """
        # We're not using message IDs anymore, so this is a no-op
        logger.debug(f"set_last_processed_id called with {message_id} - using timestamps instead")
