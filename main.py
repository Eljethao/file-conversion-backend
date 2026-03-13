from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uuid
import time
import logging
from config import settings
from s3_client import s3_client
from tasks import run_conversion_task
import redis
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PDF to DOCX Converter API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True
)


class PresignedUrlRequest(BaseModel):
    filename: str
    content_type: str = "application/pdf"


class PresignedUrlResponse(BaseModel):
    upload_url: str
    file_id: str
    file_key: str


class ConversionRequest(BaseModel):
    file_id: str
    filename: str


class ConversionResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: Optional[int] = None
    download_url: Optional[str] = None
    error: Optional[str] = None


@app.get("/")
def read_root():
    return {"message": "PDF to DOCX Converter API", "status": "running"}


@app.post("/api/upload/presigned-url", response_model=PresignedUrlResponse)
def get_presigned_upload_url(request: PresignedUrlRequest):
    try:
        file_id = str(uuid.uuid4())
        file_extension = request.filename.split('.')[-1] if '.' in request.filename else 'pdf'
        file_key = f"uploads/{file_id}.{file_extension}"

        upload_url = s3_client.generate_presigned_upload_url(
            file_key=file_key,
            content_type=request.content_type
        )

        logger.info(f"Generated presigned URL for file: {request.filename}, file_id: {file_id}")

        return PresignedUrlResponse(
            upload_url=upload_url,
            file_id=file_id,
            file_key=file_key
        )

    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate upload URL: {str(e)}")


@app.post("/api/convert", response_model=ConversionResponse, status_code=202)
def start_conversion(request: ConversionRequest, background_tasks: BackgroundTasks):
    try:
        file_extension = request.filename.split('.')[-1] if '.' in request.filename else 'pdf'
        pdf_key = f"uploads/{request.file_id}.{file_extension}"

        if not s3_client.file_exists(pdf_key):
            raise HTTPException(status_code=404, detail="File not found in S3. Please upload the file first.")

        task_id = str(uuid.uuid4())
        base_filename = request.filename.rsplit('.', 1)[0] if '.' in request.filename else request.filename
        docx_key = f"converted/{task_id}_{base_filename}.docx"

        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({
                "status": "PENDING",
                "progress": 0,
                "task_id": task_id,
                "created_at": time.time()
            })
        )

        background_tasks.add_task(run_conversion_task, task_id, pdf_key, docx_key)

        logger.info(f"Started conversion task: {task_id} for file: {request.filename}")

        return ConversionResponse(
            task_id=task_id,
            status="PENDING",
            message="Conversion task started successfully"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting conversion: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start conversion: {str(e)}")


@app.get("/api/status/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str):
    try:
        task_data = redis_client.get(f"task:{task_id}")

        if not task_data:
            raise HTTPException(status_code=404, detail="Task not found")

        task_info = json.loads(task_data)
        status = task_info.get("status", "UNKNOWN")

        # Surface stale tasks stuck in PENDING (e.g. server restart mid-task)
        STALE_PENDING_SECONDS = 120
        if status == "PENDING":
            created_at = task_info.get("created_at")
            if created_at and (time.time() - created_at) > STALE_PENDING_SECONDS:
                status = "TIMEOUT"
                task_info["error"] = (
                    "Conversion did not start within 2 minutes. Please try again."
                )

        return TaskStatusResponse(
            task_id=task_id,
            status=status,
            progress=task_info.get("progress"),
            download_url=task_info.get("download_url"),
            error=task_info.get("error")
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@app.get("/health")
def health_check():
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}
