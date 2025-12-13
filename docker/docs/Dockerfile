# SAGE Documentation Dockerfile
# MkDocs Material documentation server
# =====================================

FROM python:3.11-slim

# Set working directory
WORKDIR /docs

# Install MkDocs with Material theme
RUN pip install --no-cache-dir \
    mkdocs>=1.5.0 \
    mkdocs-material>=9.4.0 \
    mkdocs-git-revision-date-localized-plugin>=1.2.0 \
    pymdown-extensions>=10.0

# Copy documentation source
COPY . .

# Create mkdocs.yml if it doesn't exist
RUN if [ ! -f mkdocs.yml ]; then \
    echo "site_name: SAGE Documentation" > mkdocs.yml && \
    echo "theme:" >> mkdocs.yml && \
    echo "  name: material" >> mkdocs.yml; \
    fi

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8080 || exit 1

# Expose port
EXPOSE 8080

# Run MkDocs server
CMD ["mkdocs", "serve", "--dev-addr=0.0.0.0:8080"]
