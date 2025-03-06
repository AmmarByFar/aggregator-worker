from typing import List, Dict, Any, Type
from loguru import logger

from src.config import Config
from src.models import RawMessage, NewsItem
from src.sources.base import BaseSource
from src.sources.telegram import TelegramSource
from src.sources.twitter import TwitterSource
from src.sources.facebook import FacebookSource
from src.processors.llm_processor import LLMProcessor
from src.storage.supabase_client import SupabaseClient

class Aggregator:
    def __init__(self, config: Config):
        self.config = config
        self.sources: Dict[str, BaseSource] = {}
        self.llm_processor = LLMProcessor(config)
        self.storage = SupabaseClient(config)
        
        # Initialize enabled sources
        self._initialize_sources()
    
    def _initialize_sources(self):
        """Initialize the enabled data sources based on configuration"""
        source_mapping = {
            "telegram": TelegramSource,
            "twitter": TwitterSource,
            "facebook": FacebookSource,
        }
        
        for source_name in self.config.worker_sources:
            if source_name in source_mapping:
                try:
                    logger.info(f"Initializing {source_name} source")
                    self.sources[source_name] = source_mapping[source_name](self.config)
                except Exception as e:
                    logger.error(f"Failed to initialize {source_name} source: {e}")
            else:
                logger.warning(f"Unknown source: {source_name}")
    
    def run(self):
        """Run a complete aggregation cycle"""
        # Collect raw messages from all sources
        raw_messages = self._collect_messages()
        if not raw_messages:
            logger.info("No new messages collected")
            return
        
        logger.info(f"Collected {len(raw_messages)} raw messages")
        
        # Process messages with LLM
        news_items = self._process_messages(raw_messages)
        if not news_items:
            logger.info("No valid news items found")
            return
        
        logger.info(f"Processed {len(news_items)} valid news items")
        
        # Store news items
        self._store_news_items(news_items)
    
    def _collect_messages(self) -> List[RawMessage]:
        """Collect messages from all enabled sources"""
        all_messages = []
        
        for source_name, source in self.sources.items():
            try:
                logger.info(f"Collecting messages from {source_name}")
                messages = source.collect_messages()
                logger.info(f"Collected {len(messages)} messages from {source_name}")
                all_messages.extend(messages)
            except Exception as e:
                logger.error(f"Error collecting messages from {source_name}: {e}")
        
        return all_messages
    
    def _process_messages(self, raw_messages: List[RawMessage]) -> List[NewsItem]:
        """Process raw messages with LLM to extract news items"""
        news_items = []
        
        for message in raw_messages:
            try:
                result = self.llm_processor.process_message(message)
                if result:
                    news_items.append(result)
            except Exception as e:
                logger.error(f"Error processing message {message.source_id} from {message.source}: {e}")
        
        return news_items
    
    def _store_news_items(self, news_items: List[NewsItem]) -> None:
        """Store processed news items in Supabase"""
        try:
            self.storage.store_news_items(news_items)
            logger.info(f"Successfully stored {len(news_items)} news items")
        except Exception as e:
            logger.error(f"Error storing news items: {e}")
