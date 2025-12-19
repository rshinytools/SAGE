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
    from core.engine.clinical_naming import get_naming_service, ClinicalNamingService
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

async def stream_claude_response(prompt: str):
    """
    Stream response from Claude API as fallback when pipeline not available.

    This is only used when the inference pipeline fails to initialize.
    In normal operation, all queries go through the pipeline.
    """
    import anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY")
    model = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

    if not api_key:
        yield "Error: Claude API key not configured. Please set ANTHROPIC_API_KEY."
        return

    # System prompt for SAGE
    system_prompt = """You are SAGE (Study Analytics Generative Engine), an AI assistant
specialized in clinical trial data analysis. You help users query clinical data using
natural language and provide clear, accurate responses.

Note: The full inference pipeline is currently unavailable, so I cannot execute actual
data queries. I can still help with general questions about clinical data analysis.

Be concise, professional, and helpful. If you're unsure about something, say so."""

    try:
        client = anthropic.Anthropic(api_key=api_key)

        with client.messages.stream(
            model=model,
            max_tokens=2048,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}]
        ) as stream:
            for text in stream.text_stream:
                yield text

    except anthropic.APIConnectionError:
        yield "Error: Unable to connect to Claude API. Please check your network connection."
    except anthropic.AuthenticationError:
        yield "Error: Invalid Claude API key. Please check your ANTHROPIC_API_KEY."
    except Exception as e:
        yield f"Error: {str(e)}"


def generate_title_from_message(message: str) -> str:
    """Generate a conversation title from the first message."""
    # Take first 50 chars and clean up
    title = message[:50].strip()
    if len(message) > 50:
        title += "..."
    return title


def get_available_tables_from_connection() -> dict:
    """Get available tables and columns from the shared DuckDB connection."""
    try:
        # Import here to avoid circular dependency
        from routers.data import get_duckdb_connection
        conn = get_duckdb_connection()
        if conn is None:
            logger.warning("DuckDB connection not available")
            return {}

        tables = {}
        # Get all tables
        result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """)
        for row in result.fetchall():
            table_name = row[0]
            # Skip system/internal tables
            if table_name.startswith('_') or table_name.startswith('meddra'):
                continue
            # Get columns for each table
            cols = conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            tables[table_name] = [c[0] for c in cols.fetchall()]

        logger.info(f"Loaded {len(tables)} tables from DuckDB: {list(tables.keys())}")
        return tables
    except Exception as e:
        logger.error(f"Error getting tables from DuckDB: {e}")
        return {}


def get_pipeline() -> Optional[InferencePipeline]:
    """Get or create the inference pipeline instance."""
    global _pipeline_instance

    if not PIPELINE_AVAILABLE:
        return None

    if _pipeline_instance is None:
        try:
            db_path = os.getenv("DUCKDB_PATH", "/app/data/clinical.duckdb")
            metadata_path = os.getenv("METADATA_PATH", "/app/knowledge/golden_metadata.json")
            use_mock = os.getenv("USE_MOCK_PIPELINE", "false").lower() == "true"

            # Factory 3 fuzzy index path
            knowledge_dir = os.getenv("KNOWLEDGE_DIR", "/app/knowledge")
            fuzzy_index_path = os.path.join(knowledge_dir, "fuzzy_index.pkl")

            # Get available tables and shared connection to avoid lock conflicts
            from routers.data import get_duckdb_connection
            db_conn = get_duckdb_connection()
            available_tables = get_available_tables_from_connection()

            _pipeline_instance = create_pipeline(
                db_path=db_path,
                metadata_path=metadata_path,
                use_mock=use_mock,
                fuzzy_index_path=fuzzy_index_path,
                auto_load_factory3=True,  # Enable Factory 3/3.5 integration
                available_tables=available_tables,
                db_connection=db_conn
            )
            logger.info("Inference pipeline initialized successfully with Claude + Factory 3/3.5")
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            import traceback
            traceback.print_exc()
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
        columns = list(result.data[0].keys()) if result.data else []
        response["columns"] = columns

        # Add friendly column labels from metadata
        # Maps technical names (AESTDTC) to friendly names (Start Date/Time of Adverse Event)
        try:
            naming_service = get_naming_service()
            if naming_service:
                # Get table name from methodology if available
                table_name = None
                if result.methodology and isinstance(result.methodology, dict):
                    table_name = result.methodology.get("table_used")

                response["column_labels"] = {
                    col: naming_service.get_column_label(col, table_name)
                    for col in columns
                }
        except Exception as e:
            logger.warning(f"Could not get column labels: {e}")
            # Fall back to technical names
            response["column_labels"] = {col: col for col in columns}

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
    """
    Send a message and get a response using the Inference Pipeline.

    This routes ALL queries through the InferencePipeline which:
    - Understands your clinical data schema
    - Applies clinical rules (ADaM > SDTM, population defaults)
    - Generates valid SQL against your actual tables
    - Formats responses in clinical-friendly language
    - Provides confidence scores and methodology
    """
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

    # Process through InferencePipeline
    pipeline = get_pipeline()
    metadata = {}

    if pipeline:
        # Use the inference pipeline for clinical data queries
        try:
            # Pass conversation ID for session context (enables follow-up questions)
            result = pipeline.process_with_session(data.message, session_id=conv_id)

            # Log audit event
            log_audit_event(
                event_type="query",
                query=data.message,
                user_id=user_id,
                result=result.to_dict() if result.success else None,
                error=result.error if not result.success else None
            )

            # Format response
            response_content = result.answer

            # Build metadata for UI (confidence, methodology, SQL, etc.)
            # Get column labels from naming service
            columns = list(result.data[0].keys()) if result.data else None
            column_labels = None
            if columns:
                try:
                    naming_service = get_naming_service()
                    if naming_service:
                        table_name = None
                        if result.methodology and isinstance(result.methodology, dict):
                            table_name = result.methodology.get("table_used")
                        column_labels = {
                            col: naming_service.get_column_label(col, table_name)
                            for col in columns
                        }
                except Exception as e:
                    logger.warning(f"Could not get column labels: {e}")

            metadata = {
                "success": result.success,
                "confidence": result.confidence,
                "methodology": result.methodology,
                "sql": result.sql,  # Available in Details for technical users
                "data": result.data,
                "row_count": result.row_count,
                "columns": columns,
                "column_labels": column_labels,
                "warnings": result.warnings,
                "execution_time_ms": result.total_time_ms,
                "pipeline_used": True
            }

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            response_content = f"I encountered an error processing your query: {str(e)}"
            metadata = {"error": str(e), "pipeline_used": True}
    else:
        # Fallback to direct Claude (not recommended - no clinical context)
        logger.warning("Pipeline not available, falling back to direct Claude")
        response_content = ""
        async for chunk in stream_claude_response(data.message):
            response_content += chunk
        metadata = {
            "model": os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
            "pipeline_used": False,
            "warning": "Pipeline not available - response may lack clinical context"
        }

    # Add assistant message
    assistant_msg = {
        "id": str(uuid.uuid4()),
        "role": "assistant",
        "content": response_content,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata,
        "conversation_id": conv_id
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
    """
    Send a message and get a streaming response using SSE.

    Uses InferencePipeline for clinical data queries, then streams
    the formatted response back to the client.
    """
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
        """Generate SSE events using InferencePipeline."""
        message_id = str(uuid.uuid4())
        start_time = datetime.now()
        full_response = ""
        metadata = {}

        try:
            pipeline = get_pipeline()

            if pipeline:
                # Send processing status
                yield f"data: {json.dumps({'type': 'status', 'status': 'Processing query...'})}\n\n"
                await asyncio.sleep(0)

                # Process through pipeline (not streaming - pipeline returns complete result)
                # Pass conversation ID for session context (enables follow-up questions)
                result = pipeline.process_with_session(data.message, session_id=conv_id)

                # Log audit event
                log_audit_event(
                    event_type="query",
                    query=data.message,
                    user_id=user_id,
                    result=result.to_dict() if result.success else None,
                    error=result.error if not result.success else None
                )

                full_response = result.answer

                # Stream the response in chunks for better UX
                chunk_size = 50
                for i in range(0, len(full_response), chunk_size):
                    chunk = full_response[i:i + chunk_size]
                    yield f"data: {json.dumps({'type': 'content', 'content': chunk})}\n\n"
                    await asyncio.sleep(0.01)  # Small delay for streaming effect

                # Build metadata
                metadata = {
                    "success": result.success,
                    "confidence": result.confidence,
                    "methodology": result.methodology,
                    "sql": result.sql,
                    "data": result.data,
                    "row_count": result.row_count,
                    "warnings": result.warnings,
                    "execution_time_ms": result.total_time_ms,
                    "pipeline_used": True
                }

            else:
                # Fallback to direct Claude streaming
                logger.warning("Pipeline not available, falling back to direct Claude")
                async for chunk in stream_claude_response(data.message):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'content', 'content': chunk})}\n\n"
                    await asyncio.sleep(0)

                metadata = {
                    "model": os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
                    "pipeline_used": False,
                    "warning": "Pipeline not available - response may lack clinical context"
                }

            # Calculate total time
            end_time = datetime.now()
            if "execution_time_ms" not in metadata:
                metadata["execution_time_ms"] = int((end_time - start_time).total_seconds() * 1000)

            # Send metadata
            yield f"data: {json.dumps({'type': 'metadata', 'metadata': metadata})}\n\n"

            # Save assistant message
            assistant_msg = {
                "id": message_id,
                "role": "assistant",
                "content": full_response,
                "timestamp": datetime.now().isoformat(),
                "metadata": metadata,
                "conversation_id": conv_id
            }
            messages_db[conv_id].append(assistant_msg)

            # Update conversation
            conversations_db[conv_id]["updated_at"] = datetime.now().isoformat()

            # Send done event
            yield f"data: {json.dumps({'type': 'done', 'conversation_id': conv_id, 'message_id': message_id})}\n\n"

        except Exception as e:
            logger.error(f"Stream error: {e}")
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
    column_labels: Optional[Dict[str, str]] = None  # Friendly column names from metadata
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
        # Fall back to direct Claude if pipeline not available
        logger.warning("Pipeline not available, falling back to direct Claude")
        response_content = ""
        async for chunk in stream_claude_response(request.query):
            response_content += chunk

        # Add assistant message
        message_id = str(uuid.uuid4())
        assistant_msg = {
            "id": message_id,
            "role": "assistant",
            "content": response_content,
            "timestamp": datetime.now().isoformat(),
            "metadata": {"model": os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")}
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
        # Pass conversation ID for session context (enables follow-up questions)
        result = pipeline.process_with_session(request.query, session_id=conv_id)
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
            columns=formatted.get("columns"),
            column_labels=formatted.get("column_labels"),
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
                # Fall back to direct Claude LLM
                yield f"data: {json.dumps({'type': 'stage', 'stage': 'llm_fallback', 'message': 'Using direct Claude (pipeline not available)'})}\n\n"

                full_response = ""
                async for chunk in stream_claude_response(request.query):
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
            # Pass conversation ID for session context (enables follow-up questions)
            result = pipeline.process_with_session(request.query, session_id=conv_id)

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
