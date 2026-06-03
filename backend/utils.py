from fastapi import Header, HTTPException

AUTH_TOKEN = "changeme"

def verify_token(authorization: str = Header(None)):
    if authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(401, "Invalid or missing token")
