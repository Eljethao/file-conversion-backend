from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    PRESIGNED_URL_EXPIRATION: int = 3600
    DOWNLOAD_URL_EXPIRATION: int = 3600

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
