@app.post("/submit-lead")
async def submit_lead(data: dict):
    """Save lead data to leads.json and send email notification with failover."""
    try:
        LEADS_FILE = Path("leads.json")
        cfg = get_config()
        
        entry = {
            "name":      data.get("name", ""),
            "email":     data.get("email", ""),
            "whatsapp":  data.get("whatsapp", ""),
            "message":   data.get("message", ""),
            "session_id": data.get("session_id", ""),
            "timestamp":  datetime.now().isoformat(),
        }
        
        # Log to server console for immediate visibility
        logger.info(f"*** LIVE LEAD TEST ***: {entry['name']} ({entry['email']})")
        
        existing = []
        if LEADS_FILE.exists():
            try: existing = json.loads(LEADS_FILE.read_text())
            except: pass
        existing.append(entry)
        LEADS_FILE.write_text(json.dumps(existing, indent=2))
        
        # Trigger Email Notification
        asyncio.create_task(send_lead_email(entry, cfg))
        
        return {"success": True, "message": "Lead captured successfully"}
    except Exception as e:
        logger.error(f"Lead capture error: {e}")
        return JSONResponse({"detail": str(e)}, status_code=500)
