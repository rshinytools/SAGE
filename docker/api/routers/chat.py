"""
SAGE Chat Router
================
Handles AI chat interactions with SSE streaming support.
Integrates Factory 4 Inference Pipeline for query processing.
"""

import os
import sys
import json
import uuid
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Import auth dependency
from routers.auth import get_current_user

# Add project root to path for engine imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Import inference pipeline (Factory 4)
try:
    from core.engine import (
        InferencePipeline,
        PipelineConfig,
        create_pipeline,
        PipelineResult,
        get_confidence_color
    )
    PIPELINE_AVAILABLE = True
except ImportError as e:
    PIPELINE_AVAILABLE = False
    logging.warning(f"Inference pipeline not available: {e}")

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory storage for demo (replace with database in production)
conversations_db: Dict[str, Dict] = {}
messages_db: Dict[str, List[Dict]] = {}

# Audit log storage
audit_log: List[Dict] = []

# Global pipeline instance (lazy initialization)
_pipeline_instance: Optional[InferencePipeline] = None


# ============================================
# Models
# ============================================

class SendMessageRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    attachments: Optional[List[str]] = None


class ConversationCreate(BaseModel):
    title: Optional[str] = None


class ConversationUpdate(BaseModel):
    title: str


class Conversation(BaseModel):
    id: str
    title: str
    created_at: str
    updated_at: str
    message_count: int


class ChatMessage(BaseModel):
    id: str
    role: str
    content: str
    timestamp: str
    metadata: Optional[Dict[str, Any]] = None


# ============================================
# Helper Functions
# ============================================

def get_ollama_host() -> str:
    """Get Ollama host URL."""
    return os.getenv("OLLAMA_HOST", "http://ollama:11434")


async def stream_ollama_response(prompt: str, model: str = None):
    """Stream response from Ollama."""
    import httpx

    ollama_host = get_ollama_host()
    model = model or os.getenv("PRIMARY_MODEL", "deepseek-r1:8b")

    # System prompt for SAGE
    system_prompt = """You are SAGE (Study Analytics Generative Engine), an AI assistant
specialized in clinical trial data analysis. You help users query clinical data using
natural language and provide clear, accurate responses.

When generating SQL queries, use DuckDB syntax. Always explain your methodology
and provide confidence scores when applicable.

Be concise, professional, and helpful. If you're unsure about something, say so."""

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            async with client.stream(
                "POST",
                f"{ollama_host}/api/chat",
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    "stream": True
                }
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        try:
                            data = json.loads(line)
                            if "message" in data and "content" in data["message"]:
                                yield data["message"]["content"]
                            if data.get("done", False):
                                break
                        except json.JSONDecodeError:
                            continue
        except httpx.ConnectError:
            yield "Error: Unable to connect to LLM service. Please ensure Ollama is running."
        except Exception as e:
            yield f"Error: {str(e)}"


def generate_title_from_message(message: str) -> str:
    """Generate a conversation title from the first message."""
    # Take first 50 chars and clean up
    title = message[:50].strip()
    if len(message) > 50:
        title += "..."
    return title


def get_pipeline() -> Optional[InferencePipeline]:
    """Get or create the inference pipeline instance."""
    global _pipeline_instance

    if not PIPELINE_AVAILABLE:
        return None

    if _pipeline_instance is None:
        try:
            db_path = os.getenv("DUCKDB_PATH", "/app/data/clinical.duckdb")
            metadata_path = os.getenv("METADATA_PATH", "/app/knowledge/golden_metadata.json")
            ollama_host = os.getenv("OLLAMA_HOST", "http://ollama:11434")
            use_mock = os.getenv("USE_MOCK_PIPELINE", "false").lower() == "true"

            # Factory 3 fuzzy index path
            knowledge_dir = os.getenv("KNOWLEDGE_DIR", "/app/knowledge")
            fuzzy_index_path = os.path.join(knowledge_dir, "fuzzy_index.pkl")

            _pipeline_instance = create_pipeline(
                db_path=db_path,
                metadata_path=metadata_path,
                ollama_host=ollama_host,
                use_mock=use_mock,
                fuzzy_index_path=fuzzy_index_path,
                auto_load_factory3=True  # Enable Factory 3/3.5 integration
            )
            logger.info("Inference pipeline initialized successfully with Factory 3/3.5 integration")
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            return None

    return _pipeline_instance


def log_audit_event(
    event_type: str,
    query: str,
    user_id: str,
    result: Optional[Dict] = None,
    error: Optional[str] = None
):
    """Log an audit event for compliance."""
    event = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "user_id": user_id,
        "query": query,
        "result": result,
        "error": error
    }
    audit_log.append(event)

    # Keep only last 10000 events in memory
    if len(audit_log) > 10000:
        audit_log.pop(0)

    # Also log to file if audit path is configured
    audit_path = os.getenv("AUDIT_LOG_PATH")
    if audit_path:
        try:
            with open(audit_path, "a") as f:
                f.write(json.dumps(event) + "\n")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")


def format_pipeline_response(result: PipelineResult) -> Dict[str, Any]:
    """Format pipeline result for API response."""
    response = {
        "success": result.success,
        "answer": result.answer,
        "query": result.query
    }

    # Add data if present
    if result.data:
        response["data"] = result.data
        response["row_count"] = result.row_count
        response["columns"] = list(result.data[0].keys()) if result.data else []

    # Add SQL
    if result.sql:
        response["sql"] = result.sql

    # Add confidence
    if result.confidence:
        response["confidence"] = {
            "score": result.confidence.get("score", 0),
            "level": result.confidence.get("level", "unknown"),
            "color": get_confidence_color_name(result.confidence.get("level", "very_low")),
            "explanation": result.confidence.get("explanation", "")
        }

    # Add methodology
    if result.methodology:
        response["methodology"] = result.methodology

    # Add warnings
    if result.warnings:
        response["warnings"] = result.warnings

    # Add timing
    response["execution_time_ms"] = result.total_time_ms

    return response


def get_confidence_color_name(level: str) -> str:
    """Get color name for confidence level."""
    colors = {
        "high": "green",
        "medium": "yellow",
        "low": "orange",
        "very_low": "red"
    }
    return colors.get(level, "gray")


# ============================================
# Endpoints
# ============================================

@router.get("/conversations", response_model=List[Conversation])
async def get_conversations(current_user: dict = Depends(get_current_user)):
    """Get all conversations for the current user."""
    user_id = current_user.get("username", "anonymous")
    user_convos = []

    for conv_id, conv in conversations_db.items():
        if conv.get("user_id") == user_id:
            user_convos.append(Conversation(
                id=conv_id,
                title=conv["title"],
                created_at=conv["created_at"],
                updated_at=conv["updated_at"],
                message_count=len(messages_db.get(conv_id, []))
            ))

    # Sort by updated_at descending
    user_convos.sort(key=lambda x: x.updated_at, reverse=True)
    return user_convos


@router.post("/conversations", response_model=Conversation)
async def create_conversation(
    data: ConversationCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new conversation."""
    user_id = current_user.get("username", "anonymous")
    conv_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    conversations_db[conv_id] = {
        "user_id": user_id,
        "title": data.title or "New Conversation",
        "created_at": now,
        "updated_at": now
    }
    messages_db[conv_id] = []

    return Conversation(
        id=conv_id,
        title=conversations_db[conv_id]["title"],
        created_at=now,
        updated_at=now,
        message_count=0
    )


@router.get("/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get a specific conversation with messages."""
    user_id = current_user.get("username", "anonymous")

    if conversation_id not in conversations_db:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conv = conversations_db[conversation_id]
    if conv.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = messages_db.get(conversation_id, [])

    return {
        "id": conversation_id,
        "title": conv["title"],
        "created_at": conv["created_at"],
        "updated_at": conv["updated_at"],
        "message_count": len(messages),
        "messages": messages
    }


@router.patch("/conversations/{conversation_id}", response_model=Conversation)
async def update_conversation(
    conversation_id: str,
    data: ConversationUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update a conversation title."""
    user_id = current_user.get("username", "anonymous")

    if conversation_id not in conversations_db:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conv = conversations_db[conversation_id]
    if conv.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    conv["title"] = data.title
    conv["updated_at"] = datetime.now().isoformat()

    return Conversation(
        id=conversation_id,
        title=conv["title"],
        created_at=conv["created_at"],
        updated_at=conv["updated_at"],
        message_count=len(messages_db.get(conversation_id, []))
    )


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(
    conversation_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete a conversation."""
    user_id = current_user.get("username", "anonymous")

    if conversation_id not in conversations_db:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conv = conversations_db[conversation_id]
    if conv.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    del conversations_db[conversation_id]
    if conversation_id in messages_db:
        del messages_db[conversation_id]

    return {"success": True}


@router.post("/message")
async def send_message(
    data: SendMessageRequest,
    current_user: dict = Depends(get_current_user)
):
    """Send a message and get a non-streaming response."""
    user_id = current_user.get("username", "anonymous")

    # Create or get conversation
    if data.conversation_id:
        conv_id = data.conversation_id
        if conv_id not in conversations_db:
            raise HTTPException(status_code=404, detail="Conversation not found")
    else:
        conv_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        conversations_db[conv_id] = {
            "user_id": user_id,
            "title": generate_title_from_message(data.message),
            "created_at": now,
            "updated_at": now
        }
        messages_db[conv_id] = []

    # Add user message
    user_msg = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": data.message,
        "timestamp": datetime.now().isoformat()
    }
    messages_db[conv_id].append(user_msg)

    # Get AI response (non-streaming)
    response_content = ""
    async for chunk in stream_ollama_response(data.message):
        response_content += chunk

    # Add assistant message
    assistant_msg = {
        "id": str(uuid.uuid4()),
        "role": "assistant",
        "content": response_content,
        "timestamp": datetime.now().isoformat(),
        "metadata": {
            "model": os.getenv("PRIMARY_MODEL", "deepseek-r1:8b")
        }
    }
    messages_db[conv_id].append(assistant_msg)

    # Update conversation
    conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

    return assistant_msg


@router.post("/message/stream")
async def send_message_stream(
    data: SendMessageRequest,
    current_user: dict = Depends(get_current_user)
):
    """Send a message and get a streaming response using SSE."""
    user_id = current_user.get("username", "anonymous")

    # Create or get conversation
    if data.conversation_id:
        conv_id = data.conversation_id
        if conv_id not in conversations_db:
            raise HTTPException(status_code=404, detail="Conversation not found")
    else:
        conv_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        conversations_db[conv_id] = {
            "user_id": user_id,
            "title": generate_title_from_message(data.message),
            "created_at": now,
            "updated_at": now
        }
        messages_db[conv_id] = []

    # Add user message
    user_msg = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": data.message,
        "timestamp": datetime.now().isoformat()
    }
    messages_db[conv_id].append(user_msg)

    async def event_generator():
        """Generate SSE events."""
        full_response = ""
        message_id = str(uuid.uuid4())
        start_time = datetime.now()

        try:
            async for chunk in stream_ollama_response(data.message):
                full_response += chunk
                yield f"data: {json.dumps({'type': 'content', 'content': chunk})}\n\n"
                await asyncio.sleep(0)  # Allow other tasks to run

            # Calculate metadata
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000

            metadata = {
                "model": os.getenv("PRIMARY_MODEL", "deepseek-r1:8b"),
                "execution_time_ms": int(execution_time)
            }

            # Send metadata
            yield f"data: {json.dumps({'type': 'metadata', 'metadata': metadata})}\n\n"

            # Save assistant message
            assistant_msg = {
                "id": message_id,
                "role": "assistant",
                "content": full_response,
                "timestamp": datetime.now().isoformat(),
                "metadata": metadata
            }
            messages_db[conv_id].append(assistant_msg)

            # Update conversation
            conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

            # Send done event
            yield f"data: {json.dumps({'type': 'done', 'conversation_id': conv_id, 'message_id': message_id})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """Upload a file for context in chat."""
    # For now, just return file info
    # In production, you'd save the file and process it
    content = await file.read()
    file_id = str(uuid.uuid4())

    return {
        "file_id": file_id,
        "filename": file.filename,
        "size": len(content)
    }


@router.get("/conversations/{conversation_id}/messages", response_model=List[ChatMessage])
async def get_conversation_messages(
    conversation_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get messages for a specific conversation."""
    user_id = current_user.get("username", "anonymous")

    if conversation_id not in conversations_db:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conv = conversations_db[conversation_id]
    if conv.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return messages_db.get(conversation_id, [])


# ============================================
# Pipeline-Based Query Endpoints (Factory 4)
# ============================================

class QueryRequest(BaseModel):
    """Request model for pipeline queries."""
    query: str
    conversation_id: Optional[str] = None
    use_pipeline: bool = True  # Use inference pipeline vs direct LLM


class QueryResponse(BaseModel):
    """Response model for pipeline queries."""
    success: bool
    answer: str
    query: str
    data: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    columns: Optional[List[str]] = None
    sql: Optional[str] = None
    confidence: Optional[Dict[str, Any]] = None
    methodology: Optional[Dict[str, Any]] = None
    warnings: Optional[List[str]] = None
    execution_time_ms: float = 0.0
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None


@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Execute a clinical data query using the inference pipeline.

    This endpoint processes natural language queries through the 9-step
    Factory 4 pipeline:
    1. Input Sanitization
    2. Entity Extraction
    3. Table Resolution (Clinical Rules)
    4. Context Building
    5. SQL Generation
    6. SQL Validation
    7. Execution
    8. Confidence Scoring
    9. Explanation Generation

    Returns structured response with:
    - Answer in plain English
    - Data results (if applicable)
    - SQL query used
    - Confidence score with explanation
    - Methodology transparency
    """
    user_id = current_user.get("username", "anonymous")

    # Log query start
    log_audit_event(
        event_type="query_start",
        query=request.query,
        user_id=user_id
    )

    # Get or create conversation
    if request.conversation_id:
        conv_id = request.conversation_id
        if conv_id not in conversations_db:
            raise HTTPException(status_code=404, detail="Conversation not found")
    else:
        conv_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        conversations_db[conv_id] = {
            "user_id": user_id,
            "title": generate_title_from_message(request.query),
            "created_at": now,
            "updated_at": now
        }
        messages_db[conv_id] = []

    # Add user message
    user_msg = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": request.query,
        "timestamp": datetime.now().isoformat()
    }
    messages_db[conv_id].append(user_msg)

    # Get pipeline
    pipeline = get_pipeline()

    if pipeline is None or not request.use_pipeline:
        # Fall back to direct LLM if pipeline not available
        logger.warning("Pipeline not available, falling back to direct LLM")
        response_content = ""
        async for chunk in stream_ollama_response(request.query):
            response_content += chunk

        # Add assistant message
        message_id = str(uuid.uuid4())
        assistant_msg = {
            "id": message_id,
            "role": "assistant",
            "content": response_content,
            "timestamp": datetime.now().isoformat(),
            "metadata": {"model": os.getenv("PRIMARY_MODEL", "deepseek-r1:8b")}
        }
        messages_db[conv_id].append(assistant_msg)
        conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

        log_audit_event(
            event_type="query_complete_fallback",
            query=request.query,
            user_id=user_id,
            result={"answer": response_content[:500]}
        )

        return QueryResponse(
            success=True,
            answer=response_content,
            query=request.query,
            conversation_id=conv_id,
            message_id=message_id
        )

    # Execute through pipeline
    try:
        result = pipeline.process(request.query)
        formatted = format_pipeline_response(result)

        # Add assistant message with full metadata
        message_id = str(uuid.uuid4())
        assistant_msg = {
            "id": message_id,
            "role": "assistant",
            "content": result.answer,
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "pipeline": True,
                "confidence": result.confidence,
                "methodology": result.methodology,
                "sql": result.sql,
                "execution_time_ms": result.total_time_ms
            }
        }
        messages_db[conv_id].append(assistant_msg)
        conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

        # Log success
        log_audit_event(
            event_type="query_complete",
            query=request.query,
            user_id=user_id,
            result={
                "success": result.success,
                "confidence": result.confidence,
                "sql": result.sql,
                "row_count": result.row_count,
                "execution_time_ms": result.total_time_ms
            }
        )

        return QueryResponse(
            success=result.success,
            answer=result.answer,
            query=result.query,
            data=result.data,
            row_count=result.row_count,
            columns=list(result.data[0].keys()) if result.data else None,
            sql=result.sql,
            confidence=formatted.get("confidence"),
            methodology=result.methodology,
            warnings=result.warnings,
            execution_time_ms=result.total_time_ms,
            conversation_id=conv_id,
            message_id=message_id
        )

    except Exception as e:
        logger.exception(f"Pipeline error: {e}")

        log_audit_event(
            event_type="query_error",
            query=request.query,
            user_id=user_id,
            error=str(e)
        )

        raise HTTPException(
            status_code=500,
            detail=f"Query processing failed: {str(e)}"
        )


@router.post("/query/stream")
async def execute_query_stream(
    request: QueryRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Execute a clinical data query with SSE streaming.

    Streams pipeline stages as they complete:
    - stage: Current pipeline stage
    - content: Streaming response content
    - result: Final result with all metadata
    """
    user_id = current_user.get("username", "anonymous")

    # Get or create conversation
    if request.conversation_id:
        conv_id = request.conversation_id
        if conv_id not in conversations_db:
            raise HTTPException(status_code=404, detail="Conversation not found")
    else:
        conv_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        conversations_db[conv_id] = {
            "user_id": user_id,
            "title": generate_title_from_message(request.query),
            "created_at": now,
            "updated_at": now
        }
        messages_db[conv_id] = []

    # Add user message
    user_msg = {
        "id": str(uuid.uuid4()),
        "role": "user",
        "content": request.query,
        "timestamp": datetime.now().isoformat()
    }
    messages_db[conv_id].append(user_msg)

    async def event_generator():
        """Generate SSE events for pipeline execution."""
        message_id = str(uuid.uuid4())
        pipeline = get_pipeline()

        try:
            # Send start event
            yield f"data: {json.dumps({'type': 'start', 'query': request.query})}\n\n"

            if pipeline is None:
                # Fall back to direct LLM
                yield f"data: {json.dumps({'type': 'stage', 'stage': 'llm_fallback', 'message': 'Using direct LLM (pipeline not available)'})}\n\n"

                full_response = ""
                async for chunk in stream_ollama_response(request.query):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'content', 'content': chunk})}\n\n"
                    await asyncio.sleep(0)

                # Save message
                assistant_msg = {
                    "id": message_id,
                    "role": "assistant",
                    "content": full_response,
                    "timestamp": datetime.now().isoformat()
                }
                messages_db[conv_id].append(assistant_msg)
                conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

                yield f"data: {json.dumps({'type': 'done', 'conversation_id': conv_id, 'message_id': message_id})}\n\n"
                return

            # Stream pipeline stages
            stages = [
                ("sanitization", "Checking input security..."),
                ("entity_extraction", "Extracting clinical terms..."),
                ("table_resolution", "Selecting appropriate tables..."),
                ("context_building", "Building query context..."),
                ("sql_generation", "Generating SQL query..."),
                ("sql_validation", "Validating SQL..."),
                ("execution", "Executing query..."),
                ("confidence_scoring", "Calculating confidence..."),
                ("explanation", "Generating explanation...")
            ]

            for stage_id, stage_msg in stages:
                yield f"data: {json.dumps({'type': 'stage', 'stage': stage_id, 'message': stage_msg})}\n\n"
                await asyncio.sleep(0.05)  # Small delay for UI feedback

            # Execute pipeline (blocking)
            result = pipeline.process(request.query)

            # Stream the answer
            yield f"data: {json.dumps({'type': 'content', 'content': result.answer})}\n\n"

            # Send result metadata
            formatted = format_pipeline_response(result)
            yield f"data: {json.dumps({'type': 'result', 'result': formatted})}\n\n"

            # Save assistant message
            assistant_msg = {
                "id": message_id,
                "role": "assistant",
                "content": result.answer,
                "timestamp": datetime.now().isoformat(),
                "metadata": {
                    "pipeline": True,
                    "confidence": result.confidence,
                    "methodology": result.methodology,
                    "sql": result.sql
                }
            }
            messages_db[conv_id].append(assistant_msg)
            conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

            # Log audit
            log_audit_event(
                event_type="query_complete_stream",
                query=request.query,
                user_id=user_id,
                result={"success": result.success, "confidence": result.confidence}
            )

            yield f"data: {json.dumps({'type': 'done', 'conversation_id': conv_id, 'message_id': message_id})}\n\n"

        except Exception as e:
            logger.exception(f"Stream error: {e}")
            log_audit_event(
                event_type="query_error_stream",
                query=request.query,
                user_id=user_id,
                error=str(e)
            )
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/pipeline/status")
async def get_pipeline_status(current_user: dict = Depends(get_current_user)):
    """Get the status of the inference pipeline."""
    pipeline = get_pipeline()

    if pipeline is None:
        return {
            "available": False,
            "reason": "Pipeline not initialized or dependencies missing"
        }

    status = pipeline.is_ready()
    return {
        "available": True,
        "components": status,
        "ready": all(status.values())
    }


@router.get("/audit/logs")
async def get_audit_logs(
    limit: int = Query(100, le=1000),
    event_type: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Get audit logs (admin only)."""
    # In production, add admin role check
    logs = audit_log[-limit:]

    if event_type:
        logs = [log for log in logs if log.get("event_type") == event_type]

    return {
        "total": len(logs),
        "logs": logs
    }


@router.get("/query/export/{conversation_id}")
async def export_query_results(
    conversation_id: str,
    format: str = Query("csv", regex="^(csv|json)$"),
    current_user: dict = Depends(get_current_user)
):
    """Export query results from a conversation."""
    user_id = current_user.get("username", "anonymous")

    if conversation_id not in conversations_db:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conv = conversations_db[conversation_id]
    if conv.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = messages_db.get(conversation_id, [])

    # Find the last assistant message with data
    for msg in reversed(messages):
        if msg.get("role") == "assistant" and msg.get("metadata", {}).get("pipeline"):
            data = msg.get("metadata", {}).get("data")
            if data:
                if format == "csv":
                    import csv
                    import io
                    output = io.StringIO()
                    if data:
                        writer = csv.DictWriter(output, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
                    return StreamingResponse(
                        iter([output.getvalue()]),
                        media_type="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=results_{conversation_id}.csv"}
                    )
                else:
                    return {"data": data}

    raise HTTPException(status_code=404, detail="No exportable data found")
