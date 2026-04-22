"""Entry point for mcp dev / mcp run."""
import sys
import os

# Ensure project root is on sys.path so 'src' package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.server import mcp  # noqa: E402

if __name__ == "__main__":
    mcp.run()
