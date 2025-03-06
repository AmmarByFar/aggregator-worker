import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from loguru import logger
import tweepy
from src.config import Config
from src.models import RawMessage
from src.sources.base import BaseSource

class TwitterSource(BaseSource):
    """Source for collecting messages from Twitter/X accounts"""
    
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
        
        # Store last processed tweet IDs for each account
        self.last_processed_ids: Dict[str, str] = {}
        self._load_last_processed_ids()
    
    def _load_last_processed_ids(self) -> None:
        """Load last processed tweet IDs from storage"""
        # In a real implementation, this would load from a persistent storage
        # For now, we'll just initialize with empty values
        for account in self.config.twitter_accounts:
            self.last_processed_ids[account] = ""
    
    def collect_messages(self) -> List[RawMessage]:
        """Collect tweets from configured Twitter accounts"""
        all_messages: List[RawMessage] = []
        
        for account in self.config.twitter_accounts:
            try:
                messages = self._collect_from_account(account)
                all_messages.extend(messages)
            except Exception as e:
                logger.error(f"Error collecting tweets from Twitter account {account}: {e}")
        
        return all_messages
    
    def _collect_from_account(self, account: str) -> List[RawMessage]:
        """Collect tweets from a specific Twitter account"""
        messages: List[RawMessage] = []
        
        # Get the last processed tweet ID for this account
        last_id = self.get_last_processed_id(account)
        
        # In a real implementation, we would use the Twitter API to get tweets
        # For now, we'll just return an empty list
        
        # Example of how this would work with the Twitter API:
        """
        kwargs = {
            'screen_name': account,
            'count': 100,
            'tweet_mode': 'extended',
            'exclude_replies': False,
            'include_rts': True
        }
        
        if last_id:
            kwargs['since_id'] = last_id
        
        tweets = self.client.user_timeline(**kwargs)
        
        for tweet in tweets:
            # Use full_text for the complete tweet content
            content = tweet.full_text if hasattr(tweet, 'full_text') else tweet.text
            
            message = RawMessage(
                source="twitter",
                source_id=str(tweet.id),
                content=content,
                author=tweet.user.screen_name,
                timestamp=tweet.created_at,
                metadata={
                    'account': account,
                    'tweet_id': tweet.id,
                    'retweet_count': tweet.retweet_count,
                    'favorite_count': tweet.favorite_count,
                    'is_retweet': hasattr(tweet, 'retweeted_status'),
                    'hashtags': [h['text'] for h in tweet.entities.get('hashtags', [])],
                    'urls': [u['expanded_url'] for u in tweet.entities.get('urls', [])],
                    'user_mentions': [m['screen_name'] for m in tweet.entities.get('user_mentions', [])],
                }
            )
            messages.append(message)
            
            # Update last processed ID
            if not last_id or int(tweet.id) > int(last_id):
                self.set_last_processed_id(account, str(tweet.id))
        """
        
        logger.info(f"Collected {len(messages)} tweets from Twitter account {account}")
        return messages
    
    def get_last_processed_id(self, account: str = None) -> str:
        """Get the ID of the last processed tweet for an account"""
        if account is None:
            # If no account is specified, return the oldest ID
            if not self.last_processed_ids:
                return ""
            return min(self.last_processed_ids.values())
        
        return self.last_processed_ids.get(account, "")
    
    def set_last_processed_id(self, account: str, tweet_id: str) -> None:
        """Set the ID of the last processed tweet for an account"""
        self.last_processed_ids[account] = tweet_id
        # In a real implementation, this would persist to storage
