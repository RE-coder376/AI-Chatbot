
import sys
import os
from pathlib import Path

# Add project root to sys.path to import app
sys.path.append(os.getcwd())

from app import get_config

def debug_cfg():
    cfg = get_config("mal")
    print(f"Secondary Prompt: {cfg.get('secondary_prompt', 'NOT FOUND')}")

if __name__ == "__main__":
    debug_cfg()
