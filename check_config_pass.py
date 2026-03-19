
import json
from pathlib import Path

CONFIG_FILE = Path("config.json")
ACTIVE_DB_FILE = Path("active_db.txt")
DATABASES_DIR = Path("databases")

def get_config():
    root = {}
    if CONFIG_FILE.exists():
        try: root = json.loads(CONFIG_FILE.read_text())
        except: pass
    if not root:
        root = {"admin_password": "admin", "bot_name": "AI Assistant",
                "business_name": "Our Company", "branding": {}}
    
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    if active:
        db_cfg_file = DATABASES_DIR / active / "config.json"
        if db_cfg_file.exists():
            try:
                db_cfg = json.loads(db_cfg_file.read_text())
                for k, v in db_cfg.items():
                    if v != "" and v is not None:
                        root[k] = v
            except: pass
    return root

if __name__ == "__main__":
    cfg = get_config()
    print(f"Password in config: '{cfg.get('admin_password')}'")
