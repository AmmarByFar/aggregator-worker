from typing import Optional, Dict, Any, List
from datetime import datetime
from loguru import logger

from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers import PydanticOutputParser
from langchain.chains import LLMChain
from pydantic import BaseModel, Field

from src.config import Config
from src.models import RawMessage, NewsItem
from src.storage.supabase_client import SupabaseClient

class NewsExtraction(BaseModel):
    """Schema for LLM output parsing"""
    is_valid_news: bool = Field(description="Whether the message contains valid news")
    title: Optional[str] = Field(None, description="Title of the news message, post or article")
    content: Optional[str] = Field(None, description="Main content of the message")
    country: Optional[str] = Field(None, description="Country the news is about")
    city: Optional[str] = Field(None, description="City the news is about")
    categories: list[str] = Field(default_factory=list, description="Categories the news belongs to")
    person_names: list[str] = Field(default_factory=list, description="Names of individuals mentioned in the news content")
    # confidence_score: float = Field(description="Confidence score between 0.0 and 1.0")

    # @field_validator('confidence_score')
    # def check_confidence_score(cls, v):
    #     if not 0.0 <= v <= 1.0:
    #         raise ValueError("Confidence score must be between 0.0 and 1.0")
    #     return v

class LLMProcessor:
    """Processes raw messages using LLM to extract news information"""
    
    def __init__(self, config: Config):
        self.config = config
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0,
            openai_api_key=config.openai_api_key
        )
        
        # Initialize Supabase client for similarity search
        self.supabase = SupabaseClient(config)
        
        # Initialize output parser
        self.parser = PydanticOutputParser(pydantic_object=NewsExtraction)
        
        # Create prompt template
        template = """
        You are an AI assistant that analyzes social media messages to identify and extract news information.
        
        Analyze the following message and determine if it contains valid news. If it does, extract the relevant information.
        
        Message source: {source}
        Message timestamp: {timestamp}
        Message content: {content}
        
        Your task:
        1. Determine if this message contains valid news information.
        2. If it does, generate a very breif title or extract a title if already available. Extract the main content and identify the country and city it refers to (if applicable).
        3. Assign relevant categories to the news (e.g., politics, technology, sports, etc.).
        4. Extract the names of the individuals mentioned in the main content.
        
        {format_instructions}
        """
        
        self.prompt = PromptTemplate(
            template=template,
            input_variables=["source", "timestamp", "content"],
            partial_variables={"format_instructions": self.parser.get_format_instructions()}
        )
        
        # Create chain
        self.chain = LLMChain(llm=self.llm, prompt=self.prompt)
    
    async def process_message(self, message: RawMessage) -> Optional[NewsItem]:
        """
        Process a raw message to extract news information
        
        Args:
            message (RawMessage): The raw message to process
            
        Returns:
            Optional[NewsItem]: The extracted news item, or None if the message is not valid news
        """
        try:
            # Format message for LLM
            inputs = {
                "source": message.source,
                "timestamp": message.timestamp.isoformat(),
                "content": message.content
            }
            
            # Run LLM chain
            result = self.chain.run(inputs)
            
            # Parse result
            extraction = self.parser.parse(result)
            
            # If not valid news, return None
            if not extraction.is_valid_news:
                logger.debug(f"Message {message.source_id} from {message.source} is not valid news")
                return None
            
            # Get embedding and similarity score
            embedding, similarity_score = await self.supabase.get_embedding_and_similarity(message)

            # logger.info(f"Generated similarity_score: {similarity_score} and embedding: {embedding}")
            
            # Create news item
            # Get source URL from metadata if available
            source_url = message.metadata.get("source_url", "")
            
            news_item = NewsItem(
                title=extraction.title or "Untitled",
                content=extraction.content or message.content,
                source=message.source,
                source_id=message.source_id,
                source_url=source_url,
                author=message.author,
                country=extraction.country,
                city=extraction.city,
                timestamp=message.timestamp,
                created_at=datetime.now(),
                is_valid_news=extraction.is_valid_news,
                similarity_score=similarity_score,
                embedding=embedding,
                categories=extraction.categories,
                metadata=message.metadata,
                person_names=extraction.person_names
            )
            
            logger.info(f"Extracted news item: {news_item.title} from {message.source}")
            return news_item
            
        except Exception as e:
            logger.error(f"Error processing message {message.source_id} from {message.source}: {e}")
            return None
