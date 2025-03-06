from abc import ABC, abstractmethod
from typing import List
from src.config import Config
from src.models import RawMessage

class BaseSource(ABC):
    """Base class for all data sources"""
    
    def __init__(self, config: Config):
        self.config = config
        self.source_name = self.__class__.__name__.lower().replace('source', '')
    
    @abstractmethod
    def collect_messages(self) -> List[RawMessage]:
        """
        Collect messages from the source
        
        Returns:
            List[RawMessage]: List of raw messages collected from the source
        """
        pass
    
    @abstractmethod
    def get_last_processed_id(self) -> str:
        """
        Get the ID of the last processed message
        
        Returns:
            str: ID of the last processed message
        """
        pass
    
    @abstractmethod
    def set_last_processed_id(self, message_id: str) -> None:
        """
        Set the ID of the last processed message
        
        Args:
            message_id (str): ID of the last processed message
        """
        pass
