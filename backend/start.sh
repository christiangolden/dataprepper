#!/bin/bash
cd "$(dirname "$0")"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000