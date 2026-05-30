from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
import asyncio
import logging

import phos_queue as q

app = FastAPI()
logger = logging.getLogger('phos-watch-web')
LOGFILE = 'phos_watch.log'


@app.get('/status')
async def status():
    return JSONResponse({'queue_length': q.qlen()})


@app.websocket('/ws/logs')
async def websocket_logs(ws: WebSocket):
    await ws.accept()
    try:
        # send last 200 lines as initial backlog
        try:
            with open(LOGFILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()[-200:]
        except FileNotFoundError:
            lines = []

        for line in lines:
            await ws.send_text(line.rstrip('\n'))

        # tail the file for new lines
        # open file and seek to end
        f = open(LOGFILE, 'r', encoding='utf-8') if True else None
        if f:
            f.seek(0, 2)
        try:
            while True:
                if f:
                    where = f.tell()
                    line = f.readline()
                    if not line:
                        await asyncio.sleep(0.5)
                        f.seek(where)
                        continue
                    await ws.send_text(line.rstrip('\n'))
                else:
                    await asyncio.sleep(1)
        finally:
            if f:
                f.close()
    except WebSocketDisconnect:
        pass


@app.get('/')
async def index():
    html = '''
<html>
  <head>
    <title>phos-watch Admin</title>
    <meta charset="utf-8" />
  </head>
  <body>
    <h2>phos-watch Admin</h2>
    <div>Queue length: <span id="qlen">...</span></div>
    <h3>Logs</h3>
    <pre id="logs" style="height:400px;overflow:auto;background:#111;color:#eee;padding:8px;"></pre>
    <script>
      async function updateQ() {
        try{
          const res = await fetch('/status');
          const j = await res.json();
          document.getElementById('qlen').innerText = j.queue_length;
        }catch(e){ console.error(e); }
      }
      setInterval(updateQ, 2000);
      updateQ();

      const ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://') + location.host + '/ws/logs');
      const logs = document.getElementById('logs');
      ws.onmessage = (ev)=>{ logs.textContent += ev.data + '\n'; logs.scrollTop = logs.scrollHeight; };
    </script>
  </body>
</html>
'''
    return HTMLResponse(html)
