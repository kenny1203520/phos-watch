from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import asyncio
import logging

import phos_queue as q

app = FastAPI()
logger = logging.getLogger('phos-watch-web')


@app.get('/status')
async def status():
    return JSONResponse({'queue_length': q.qlen()})


@app.websocket('/ws/logs')
async def websocket_logs(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            # placeholder: push heartbeat every 2s
            await ws.send_text('heartbeat')
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
