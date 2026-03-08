import time
import subprocess
import requests
import logging
import json
import sys
import os
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION ---
API_URL = "http://127.0.0.1:8000"
CHECK_INTERVAL = 15  # Check every 15 seconds
STARTUP_WAIT = 30    # Wait 30s for server to load heavy models
LOG_FILE = Path("healer_logs.json")
APP_LOG = Path("server_out.log")
DB_DIR = Path("databases/agentfactory")
CONFIG_FILE = DB_DIR / "config.json"
ACTIVE_DB_FILE = Path("active_db.txt")

logging.basicConfig(level=logging.INFO, format="[WATCHDOG] %(message)s")
logger = logging.getLogger("watchdog")

DEFAULT_CONFIG = {
  "admin_password": "admin123",
  "widget_key": "Q2o2oQKUEQX82eeOwtP78olsw6wwxfUVqXJRHKWvl2E",
  "business_name": "AgentFactory",
  "bot_name": "Agni",
  "topics": "AI Agents",
  "behavior": { "lead_capture_enabled": True },
  "branding": {
    "header_title": "AgentFactory Support",
    "header_subtitle": "Online — here to help",
    "logo_emoji": "🤖",
    "welcome_message": "Hi! I'm Agni, how can I help you?",
    "input_placeholder": "Type a message...",
    "primary_color": "#6366f1",
    "secondary_color": "#4f46e5",
    "bot_bubble_color": "#f1f5f9",
    "user_bubble_color": "#6366f1",
    "font_style": "Modern Sans"
  }
}

class Watchdog:
    def __init__(self):
        self.process = None

    def log_event(self, problem, action, result):
        """Logs structured events to the healer log."""
        logs = []
        if LOG_FILE.exists():
            try: logs = json.loads(LOG_FILE.read_text())
            except: logs = []
        
        event = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "problem": problem,
            "action": action,
            "result": result
        }
        logs.append(event)
        # Keep log size manageable
        LOG_FILE.write_text(json.dumps(logs[-100:], indent=2))
        logger.info(f"ACTION TAKEN: {problem} -> {action} ({result})")

    def check_files(self):
        """Ensures critical configuration files exist."""
        # 1. Check Active DB Pointer
        if not ACTIVE_DB_FILE.exists():
            ACTIVE_DB_FILE.write_text("agentfactory", encoding="utf-8")
            self.log_event("Missing active_db.txt", "Created file", "Fixed")
        
        # 2. Check Database Config
        if not DB_DIR.exists():
            DB_DIR.mkdir(parents=True, exist_ok=True)
        
        if not CONFIG_FILE.exists():
            CONFIG_FILE.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
            self.log_event("Missing config.json", "Created default config", "Fixed")
            return True # Signal that we modified config
        return False

    def start_server(self):
        """Starts the chatbot server process."""
        if self.process:
            self.stop_server()
            
        logger.info("Starting Chatbot Server...")
        try:
            self.process = subprocess.Popen(
                [sys.executable, "app.py"],
                stdout=open(APP_LOG, "a"),
                stderr=subprocess.STDOUT
            )
            time.sleep(STARTUP_WAIT) # Wait for models to load
        except Exception as e:
            logger.error(f"Failed to start server: {e}")

    def stop_server(self):
        """Kills the server process."""
        if self.process:
            self.process.kill()
            self.process = None
        # Force kill any lingering instances
        subprocess.run(["taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq app.py"], capture_output=True)

    def check_health(self):
        """Audits the running server."""
        try:
            r = requests.get(f"{API_URL}/health", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("db_status") == "missing":
                    return "DB_MISSING"
                if data.get("chunks", 0) == 0:
                    return "DB_EMPTY"
                return "OK"
            return f"HTTP_{r.status_code}"
        except requests.exceptions.ConnectionError:
            return "CONNECTION_REFUSED"
        except Exception as e:
            return f"ERROR_{str(e)}"

    def check_brain(self):
        """Audits the chat engine by sending a real query."""
        try:
            payload = {"question": "Hello", "history": []}
            headers = {"X-Widget-Key": "Q2o2oQKUEQX82eeOwtP78olsw6wwxfUVqXJRHKWvl2E"}
            r = requests.post(f"{API_URL}/chat", json=payload, headers=headers, stream=True, timeout=10)
            
            if r.status_code != 200: return f"CHAT_HTTP_{r.status_code}"
            
            # Check if we get at least one data chunk
            for line in r.iter_lines():
                if line:
                    decoded = line.decode('utf-8')
                    if "data: " in decoded:
                        return "OK"
            return "CHAT_NO_DATA"
        except Exception as e:
            return f"CHAT_ERROR_{str(e)}"

    def run(self):
        logger.info("Super-Watchdog Auditor Active. Monitoring system-wide integrity...")
        
        # Initial file audit
        if self.check_files():
            logger.info("Restored missing configuration files.")

        self.start_server()

        last_brain_check = 0
        while True:
            status = self.check_health()
            now = time.time()

            # status can be: OK, DB_MISSING, DB_EMPTY, loading, starting, CONNECTION_REFUSED
            
            if status == "CONNECTION_REFUSED":
                self.log_event("Server Unresponsive", "Restarting Server", "Pending")
                self.start_server()
            
            elif status == "loading" or status == "starting":
                # Server is initializing in background, give it time
                pass

            elif status == "OK" or status == "ready":
                # Healthy - perform Brain Audit every 2 mins instead of 1 to reduce load
                if now - last_brain_check > 120:
                    last_brain_check = now
                    brain_status = self.check_brain()
                    if brain_status != "OK" and brain_status != "CHAT_ERROR_loading":
                        self.log_event(f"Brain Failure: {brain_status}", "Forcing Repair & Restart", "Pending")
                        self.start_server()

            # Self-Correction: File check every loop
            if self.check_files():
                self.log_event("Config file anomaly", "Restoring & Restarting", "Fixed")
                self.start_server()

            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    wd = Watchdog()
    wd.run()
