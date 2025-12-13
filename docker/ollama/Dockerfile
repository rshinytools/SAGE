# SAGE Ollama Dockerfile
# Custom Ollama image with pre-configuration for clinical AI
# ============================================================

FROM ollama/ollama:latest

# Set environment variables for optimal performance
ENV OLLAMA_HOST=0.0.0.0
ENV OLLAMA_ORIGINS=*
ENV OLLAMA_KEEP_ALIVE=24h
ENV OLLAMA_NUM_PARALLEL=2

# Create directory for models
RUN mkdir -p /root/.ollama

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD curl -f http://localhost:11434/api/tags || exit 1

# Expose Ollama API port
EXPOSE 11434

# Default command
CMD ["serve"]
