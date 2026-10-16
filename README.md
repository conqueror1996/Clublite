# Clublite — Deposit Confirmer UI

Flask app with streaming (NDJSON) confirmation engine and mobile-friendly UI.

## Deploy on Render (free tier)

1. Fork/push this repo to your GitHub.
2. Render dashboard → **New → Web Service** → connect this repo.
3. Settings (auto-detected): Runtime **Python**, Start command from `Procfile`.
4. Add Environment Variables:

| Variable | Value |
|----------|-------|
| `WB_MERCHANT_ID` | `32` |
| `WB_MERCHANT_SECRET` | your merchant signing key (keep private) |
| `CONFIRMER_TOKEN` | your access key — pick any long random string |

5. Deploy → your URL: `https://<service-name>.onrender.com/?key=YOUR_CONFIRMER_TOKEN`

## Local run

```bash
pip install -r requirements.txt
export WB_MERCHANT_SECRET=...   # optional locally
python3 app.py                  # http://127.0.0.1:8901
```

## Notes

- Every request runs its own stateless pipeline (session-isolated, safe for concurrent users).
- Each request is logged under a unique session ID.
- Without `CONFIRMER_TOKEN` set, the UI is open — only run that way on localhost.
- Secrets are read from environment variables only; none are committed.
