"""Конфигурация из переменных окружения."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Claude
    anthropic_api_key: str = ""
    claude_model: str = "claude-opus-4-6"

    # Server
    api_host: str = "0.0.0.0"
    api_port: int = 8002

    # TTM (TraderMake.Money)
    ttm_api_key: str = ""
    ttm_base_url: str = "https://tradermake.money/api/v2"

    # Whisper service
    whisper_base_url: str = "http://whisper-api:8001"

    # ElevenLabs TTS
    elevenlabs_api_key: str = ""
    elevenlabs_voice_id: str = "21m00Tcm4TlvDq8ikWAM"  # Rachel (default)

    # Redis (short-term memory)
    redis_url: str = "redis://redis:6379/0"

    # Vector DB
    chroma_host: str = "chroma"
    chroma_port: int = 8000

    # Risk / Trade deposit
    # Стартовый депозит для расчёта рисков (% от депозита).
    # Позднее заменить на получение из Google Sheets / плана.
    trade_deposit: float = 1000.0
    # Параметр "далеко ниже MPP": насколько pnl_pct_depo отстаёт от mpp_pct_depo (в %)
    risk_far_below_mpp_threshold: float = 3.0


settings = Settings()
