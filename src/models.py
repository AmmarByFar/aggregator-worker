from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class RawMessage(BaseModel):
    """Raw message from a source before processing"""
    source: str  # 'telegram', 'twitter', 'facebook', etc.
    source_id: str  # Unique ID from the source
    content: str  # Raw content
    author: Optional[str] = None
    timestamp: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Additional source-specific data

class NewsItem(BaseModel):
    """Processed news item after LLM analysis"""
    id: Optional[str] = None  # Will be set when saved to Supabase
    title: str
    content: str
    source: str  # 'telegram', 'twitter', 'facebook', etc.
    source_id: str  # Unique ID from the source
    source_url: Optional[str] = None
    author: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    timestamp: datetime
    created_at: Optional[datetime] = None
    is_valid_news: bool = True
    confidence_score: float  # 0.0 to 1.0
    categories: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
