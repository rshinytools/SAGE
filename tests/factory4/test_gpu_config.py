# Tests for GPU Configuration (Step 7)
"""
Test suite for GPU configuration in SAGE deployment.

These tests verify that:
- GPU compose override files exist
- GPU_TYPE environment variable is valid
- Configuration files have correct structure
"""

import pytest
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestGPUComposeFilesExist:
    """Test that GPU compose override files exist."""

    def test_nvidia_compose_file_exists(self):
        """NVIDIA compose override should exist."""
        nvidia_file = project_root / "docker-compose.nvidia.yml"
        assert nvidia_file.exists(), "docker-compose.nvidia.yml should exist"

    def test_amd_compose_file_exists(self):
        """AMD compose override should exist."""
        amd_file = project_root / "docker-compose.amd.yml"
        assert amd_file.exists(), "docker-compose.amd.yml should exist"

    def test_base_compose_file_exists(self):
        """Base docker-compose.yml should exist."""
        base_file = project_root / "docker-compose.yml"
        assert base_file.exists(), "docker-compose.yml should exist"


class TestGPUComposeContent:
    """Test GPU compose file content."""

    def test_nvidia_has_gpu_config(self):
        """NVIDIA compose should have GPU device configuration."""
        nvidia_file = project_root / "docker-compose.nvidia.yml"
        content = nvidia_file.read_text()

        # Should have NVIDIA GPU config
        assert "nvidia" in content.lower()
        assert "gpu" in content.lower()
        assert "ollama" in content.lower()

    def test_amd_has_rocm_config(self):
        """AMD compose should have ROCm device configuration."""
        amd_file = project_root / "docker-compose.amd.yml"
        content = amd_file.read_text()

        # Should have AMD ROCm config
        assert "rocm" in content.lower()
        assert "/dev/kfd" in content or "/dev/dri" in content
        assert "ollama" in content.lower()


class TestGPUTypeEnvVar:
    """Test GPU_TYPE environment variable.

    NOTE: Since switching to Claude cloud API, GPU_TYPE is optional.
    It was previously used for local Ollama LLM inference.
    These tests verify backward compatibility but GPU_TYPE is not required.
    """

    def test_env_file_exists(self):
        """.env file should exist."""
        env_file = project_root / ".env"
        assert env_file.exists(), ".env file should exist"

    def test_gpu_type_optional(self):
        """GPU_TYPE is optional when using Claude cloud API."""
        env_file = project_root / ".env"
        content = env_file.read_text()

        # GPU_TYPE is optional now since we use Claude cloud API
        # If it exists, it should be valid
        if "GPU_TYPE" in content:
            for line in content.split('\n'):
                if line.strip().startswith('GPU_TYPE'):
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        gpu_type = parts[1].strip().lower()
                        valid_types = ['cpu', 'nvidia', 'amd']
                        assert gpu_type in valid_types, \
                            f"GPU_TYPE '{gpu_type}' should be one of {valid_types}"

    def test_claude_api_configured(self):
        """Claude API should be configured (replaces local LLM)."""
        env_file = project_root / ".env"
        content = env_file.read_text()

        assert "ANTHROPIC_API_KEY" in content or "CLAUDE" in content, \
            "Claude/Anthropic API should be configured"


class TestStartScript:
    """Test startup script configuration."""

    def test_start_script_exists(self):
        """Start script should exist."""
        # Check for PowerShell or bash script
        ps_script = project_root / "start.ps1"
        assert ps_script.exists(), "start.ps1 should exist"

    def test_start_script_has_gpu_logic(self):
        """Start script should handle GPU selection."""
        ps_script = project_root / "start.ps1"
        content = ps_script.read_text()

        # Should have GPU type checking
        assert "GPU_TYPE" in content or "gpuType" in content
        assert "nvidia" in content.lower()
        assert "amd" in content.lower()
        assert "cpu" in content.lower()


class TestLLMConfig:
    """Test LLM configuration for Claude API."""

    def test_llm_config_has_timeout(self):
        """LLM config should have reasonable timeout."""
        from core.engine.llm_providers import LLMConfig

        config = LLMConfig.from_env()
        # Timeout should be long enough for API calls
        assert config.timeout >= 30, "Timeout should be at least 30 seconds"
        # But not too long
        assert config.timeout <= 300, "Timeout should not exceed 300 seconds"


class TestEnvConfigValues:
    """Test environment configuration values."""

    def test_llm_timeout_configured(self):
        """.env should have LLM timeout configured."""
        env_file = project_root / ".env"
        content = env_file.read_text()

        assert "LLM_TIMEOUT_SECONDS" in content, \
            "LLM_TIMEOUT_SECONDS should be in .env"

    def test_query_timeout_configured(self):
        """.env should have query timeout configured."""
        env_file = project_root / ".env"
        content = env_file.read_text()

        assert "QUERY_TIMEOUT_SECONDS" in content, \
            "QUERY_TIMEOUT_SECONDS should be in .env"

    def test_timeout_values_reasonable(self):
        """Timeout values should be reasonable."""
        env_file = project_root / ".env"
        content = env_file.read_text()

        # Parse timeout values
        llm_timeout = None
        query_timeout = None

        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('LLM_TIMEOUT_SECONDS'):
                parts = line.split('=', 1)
                if len(parts) == 2:
                    try:
                        llm_timeout = int(parts[1].strip())
                    except ValueError:
                        pass
            elif line.startswith('QUERY_TIMEOUT_SECONDS'):
                parts = line.split('=', 1)
                if len(parts) == 2:
                    try:
                        query_timeout = int(parts[1].strip())
                    except ValueError:
                        pass

        if llm_timeout and query_timeout:
            # Query timeout should be greater than LLM timeout
            assert query_timeout > llm_timeout, \
                f"Query timeout ({query_timeout}) should be > LLM timeout ({llm_timeout})"
