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
    
    async def run(self):
        """Run a complete aggregation cycle"""
        # Collect raw messages from all sources
        raw_messages = self._collect_messages()
        if not raw_messages:
            logger.info("No new messages collected")
            return
        
        logger.info(f"Collected {len(raw_messages)} raw messages")
        
        # Process messages with LLM and store them immediately
        processed_count = await self._process_and_store_messages(raw_messages)
        
        if processed_count > 0:
            logger.info(f"Processed and stored {processed_count} valid news items")
        else:
            logger.info("No valid news items found")
    
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
    
    async def _process_and_store_messages(self, raw_messages: List[RawMessage]) -> int:
        """
        Process raw messages with LLM to extract news items and store each one immediately
        
        Returns:
            int: Number of successfully processed and stored news items
        """
        processed_count = 0
        
        for message in raw_messages:
            try:
                result = await self.llm_processor.process_message(message)
                if result:
                    # Store the news item immediately after processing
                    try:
                        self.storage._store_news_item(result)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Error storing news item from {message.source_id}: {e}")
            except Exception as e:
                logger.error(f"Error processing message {message.source_id} from {message.source}: {e}")
        
        return processed_count
