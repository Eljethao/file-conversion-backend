from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    # Auto-built from REDIS_* if not explicitly set in .env
    CELERY_BROKER_URL: str = ""
    CELERY_RESULT_BACKEND: str = ""

    PRESIGNED_URL_EXPIRATION: int = 3600
    DOWNLOAD_URL_EXPIRATION: int = 3600

    @model_validator(mode="after")
    def build_celery_urls(self) -> "Settings":
        redis_url = f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        if not self.CELERY_BROKER_URL:
            self.CELERY_BROKER_URL = redis_url
        if not self.CELERY_RESULT_BACKEND:
            self.CELERY_RESULT_BACKEND = redis_url
        return self

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
