"""
SAGE Chat Router
================
Handles AI chat interactions with SSE streaming support.
"""

import os
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Import auth dependency
from routers.auth import get_current_user

router = APIRouter()

# In-memory storage for demo (replace with database in production)
conversations_db: Dict[str, Dict] = {}
messages_db: Dict[str, List[Dict]] = {}


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
