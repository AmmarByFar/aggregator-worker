import os
from typing import List, Optional
from pydantic import BaseModel, Field

class Config(BaseModel):
    # Worker configuration
    worker_id: str = Field(default_factory=lambda: os.getenv("WORKER_ID", "worker1"))
    worker_sources: List[str] = Field(default_factory=lambda: os.getenv("WORKER_SOURCES", "telegram,twitter,facebook").split(","))
    polling_interval: int = Field(default_factory=lambda: int(os.getenv("POLLING_INTERVAL", "300")))
    
    # OpenAI configuration
    openai_api_key: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    
    # Supabase configuration
    supabase_url: str = Field(default_factory=lambda: os.getenv("SUPABASE_URL", ""))
    supabase_key: str = Field(default_factory=lambda: os.getenv("SUPABASE_KEY", ""))
    
    # Telegram configuration
    telegram_api_id: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_API_ID", ""))
    telegram_api_hash: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_API_HASH", ""))
    telegram_bot_token: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    telegram_phone: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_PHONE", ""))
    telegram_channels: List[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_CHANNELS", "").split(",") if os.getenv("TELEGRAM_CHANNELS") else [])
    
    # Twitter/X configuration
    twitter_api_key: Optional[str] = Field(default_factory=lambda: os.getenv("TWITTER_API_KEY", ""))
    twitter_api_secret: Optional[str] = Field(default_factory=lambda: os.getenv("TWITTER_API_SECRET", ""))
    twitter_access_token: Optional[str] = Field(default_factory=lambda: os.getenv("TWITTER_ACCESS_TOKEN", ""))
    twitter_access_secret: Optional[str] = Field(default_factory=lambda: os.getenv("TWITTER_ACCESS_SECRET", ""))
    twitter_accounts: List[str] = Field(default_factory=lambda: os.getenv("TWITTER_ACCOUNTS", "").split(",") if os.getenv("TWITTER_ACCOUNTS") else [])
    
    # Facebook configuration
    facebook_app_id: Optional[str] = Field(default_factory=lambda: os.getenv("FACEBOOK_APP_ID", ""))
    facebook_app_secret: Optional[str] = Field(default_factory=lambda: os.getenv("FACEBOOK_APP_SECRET", ""))
    facebook_access_token: Optional[str] = Field(default_factory=lambda: os.getenv("FACEBOOK_ACCESS_TOKEN", ""))
    facebook_pages: List[str] = Field(default_factory=lambda: os.getenv("FACEBOOK_PAGES", "").split(",") if os.getenv("FACEBOOK_PAGES") else [])
