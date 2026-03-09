import os
import shutil
import tempfile
import logging
from celery import Task
from celery_app import celery_app
from s3_client import s3_client
import redis
from config import settings
import json

logger = logging.getLogger(__name__)

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True
)


class ConversionTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f"Task {task_id} failed: {exc}")
        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({
                "status": "FAILED",
                "error": str(exc),
                "task_id": task_id
            })
        )


def _is_scanned_pdf(pdf_path: str) -> bool:
    """Returns True if the PDF contains no extractable text (image-only / scanned)."""
    import fitz
    doc = fitz.open(pdf_path)
    total_chars = sum(len(page.get_text("text").strip()) for page in doc)
    doc.close()
    return total_chars < 100


def _convert_text_pdf(pdf_path: str, docx_path: str):
    """
    Primary converter using pdf2docx.
    Preserves text formatting (bold, italic, underline, colour), font size/family,
    images, tables (including nested), multi-column layouts, headers/footers,
    hyperlinks, lists, indentation, alignment, and page breaks — all natively.
    """
    from pdf2docx import Converter
    cv = Converter(pdf_path)
    cv.convert(docx_path, start=0, end=None)
    cv.close()
    logger.info(f"pdf2docx conversion completed: {docx_path}")


def _convert_scanned_pdf(pdf_path: str, docx_path: str):
    """
    OCR-based converter for scanned / image-only PDFs.
    Renders each page at 200 DPI, embeds the full-page image into the DOCX
    (preserving all visual content: diagrams, charts, photos, handwriting),
    then appends the OCR-extracted text below as an editable section.
    """
    import io
    from pdf2image import convert_from_path
    from docx import Document
    from docx.shared import Inches, Pt
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    import pytesseract

    # 0.5-inch margins so the embedded image fills the page cleanly
    MARGIN_IN = 0.5
    # A4 width (8.27in) minus both margins
    IMAGE_WIDTH_IN = 8.27 - MARGIN_IN * 2

    doc = Document()
    for section in doc.sections:
        section.left_margin = Inches(MARGIN_IN)
        section.right_margin = Inches(MARGIN_IN)
        section.top_margin = Inches(MARGIN_IN)
        section.bottom_margin = Inches(MARGIN_IN)

    # Remove the default empty paragraph Word adds
    for p in list(doc.paragraphs):
        p._p.getparent().remove(p._p)

    # 200 DPI — good balance between image quality and file size
    images = convert_from_path(pdf_path, dpi=200)

    for page_num, image in enumerate(images):
        if page_num > 0:
            doc.add_page_break()

        # ── 1. Embed the full page as an image ──────────────────────────────
        # This preserves 100% of the visual: diagrams, charts, photos, stamps,
        # handwriting, signatures, watermarks — everything the OCR would miss.
        img_bytes = io.BytesIO()
        image.save(img_bytes, format="PNG")
        img_bytes.seek(0)
        doc.add_picture(img_bytes, width=Inches(IMAGE_WIDTH_IN))

        # ── 2. Run OCR and append editable text below the image ─────────────
        ocr_data = pytesseract.image_to_data(
            image, output_type=pytesseract.Output.DICT, lang="eng"
        )

        # Group words into lines keyed by (block_num, par_num, line_num)
        lines: dict = {}
        for i in range(len(ocr_data["text"])):
            word = ocr_data["text"][i].strip()
            if not word or int(ocr_data["conf"][i]) < 0:
                continue
            key = (
                ocr_data["block_num"][i],
                ocr_data["par_num"][i],
                ocr_data["line_num"][i],
            )
            if key not in lines:
                lines[key] = {
                    "words": [],
                    "top": ocr_data["top"][i],
                    "block_num": ocr_data["block_num"][i],
                }
            lines[key]["words"].append(word)

        if lines:
            # Section heading to separate image from text
            sep = doc.add_paragraph("── Extracted Text ──")
            sep.alignment = WD_ALIGN_PARAGRAPH.CENTER
            sep.runs[0].font.size = Pt(9)

            sorted_lines = sorted(
                lines.values(), key=lambda x: (x["block_num"], x["top"])
            )
            prev_block = None
            for line_data in sorted_lines:
                text = " ".join(line_data["words"])
                if not text.strip():
                    continue
                if prev_block is not None and line_data["block_num"] != prev_block:
                    doc.add_paragraph()
                para = doc.add_paragraph()
                run = para.add_run(text)
                run.font.size = Pt(11)
                prev_block = line_data["block_num"]

    doc.save(docx_path)
    logger.info(f"OCR conversion (image + text) completed: {docx_path}")


def convert_pdf_to_docx_best(pdf_path: str, docx_path: str):
    """
    Best-effort PDF → DOCX converter with automatic strategy selection:
      1. pdf2docx   — text-based PDFs (preserves all formatting, images, tables)
      2. pytesseract + pdf2image — scanned/image-only PDFs (OCR fallback)
    """
    if _is_scanned_pdf(pdf_path):
        logger.info("Detected scanned PDF — using OCR converter")
        _convert_scanned_pdf(pdf_path, docx_path)
    else:
        logger.info("Detected text-based PDF — using pdf2docx converter")
        _convert_text_pdf(pdf_path, docx_path)


@celery_app.task(bind=True, base=ConversionTask, name="tasks.convert_pdf_to_docx")
def convert_pdf_to_docx(self, task_id: str, pdf_key: str, docx_key: str):
    temp_dir = None
    pdf_path = None
    docx_path = None

    try:
        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({"status": "PROCESSING", "progress": 10, "task_id": task_id})
        )

        temp_dir = tempfile.mkdtemp()
        pdf_filename = os.path.basename(pdf_key)
        docx_filename = os.path.basename(docx_key)

        pdf_path = os.path.join(temp_dir, pdf_filename)
        docx_path = os.path.join(temp_dir, docx_filename)

        logger.info(f"Downloading PDF from S3: {pdf_key}")
        s3_client.download_file(pdf_key, pdf_path)

        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({"status": "PROCESSING", "progress": 40, "task_id": task_id})
        )

        logger.info(f"Converting PDF to DOCX: {pdf_filename}")
        convert_pdf_to_docx_best(pdf_path, docx_path)

        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({"status": "PROCESSING", "progress": 70, "task_id": task_id})
        )

        logger.info(f"Uploading DOCX to S3: {docx_key}")
        s3_client.upload_file(docx_path, docx_key)

        download_url = s3_client.generate_presigned_download_url(docx_key)

        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({
                "status": "COMPLETED",
                "progress": 100,
                "task_id": task_id,
                "download_url": download_url,
                "docx_key": docx_key
            })
        )

        logger.info(f"Task {task_id} completed successfully")

        return {
            "status": "COMPLETED",
            "task_id": task_id,
            "download_url": download_url
        }

    except Exception as e:
        logger.error(f"Error in conversion task {task_id}: {str(e)}")
        redis_client.setex(
            f"task:{task_id}",
            86400,
            json.dumps({"status": "FAILED", "error": str(e), "task_id": task_id})
        )
        raise

    finally:
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)
        if docx_path and os.path.exists(docx_path):
            os.remove(docx_path)
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
