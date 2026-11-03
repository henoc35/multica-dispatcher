"""Fake Multica server (HTTP API + realtime WebSocket) for end-to-end tests.
Serves several workspaces: data is picked from the X-Workspace-ID header (HTTP) and the
workspace_id query parameter (WebSocket), like the real server."""
import asyncio, json, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import websockets

def iso(offset_s=0):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + offset_s))

def issue(id_, ident, title, status, prio, agent, updated, status_name="", category=None, activity=None):
    d = {"id": id_, "identifier": ident, "title": title, "status": status, "status_name": status_name,
         "priority": prio, "assignee_type": "agent", "assignee_id": agent, "updated_at": updated,
         "last_activity_at": activity or updated, "revision": 1}
    if category: d["status_category"] = category
    return d

def reset():
    STATE.clear()
    STATE["reruns"] = []        # (workspace, agent, identifier, time)
    STATE["ws"] = {"ws1": ws1(), "ws2": ws2(), "ws3": ws3()}

def ws3():
    """An archived project the dispatcher must ignore when excluded."""
    return {"meta": {"id": "ws3", "slug": "archive", "name": "Archive"},
            "agents": [{"id": "be3", "name": "backend-lead", "max_concurrent_tasks": 2}],
            "tasks": {"be3": []},
            "issues": [issue("k1", "Z-1", "old project todo", "todo", "high", "be3", iso(-5000))]}

def ws2():
    """Second project: no review backlog, so its review-priority mode stays off."""
    return {"meta": {"id": "ws2", "slug": "beta", "name": "Projet Beta"},
            "agents": [{"id": "be2", "name": "backend-lead", "max_concurrent_tasks": 2},
                       {"id": "fe2", "name": "frontend-lead", "max_concurrent_tasks": 1}],
            "tasks": {"be2": [{"id": "u1", "status": "running", "issue_id": "j1", "created_at": iso(-600)}],
                      "fe2": []},
            "issues": [
                issue("j1", "Y-1", "beta busy", "in_progress", "high", "be2", iso(-600)),
                issue("j2", "Y-2", "beta todo urgent", "todo", "urgent", "be2", iso(-4000)),
                issue("j3", "Y-3", "beta todo low", "todo", "low", "be2", iso(-9000)),
                issue("j4", "Y-4", "beta front todo", "todo", "medium", "fe2", iso(-3000)),
            ]}

def ws1():
    return {
      "meta": {"id": "ws1", "slug": "alpha", "name": "Projet Alpha"},
      "agents": [
        {"id": "be", "name": "backend-lead", "max_concurrent_tasks": 4},
        {"id": "qa", "name": "qa-lead", "max_concurrent_tasks": 2},
        {"id": "mk", "name": "Mika", "max_concurrent_tasks": 1},
      ],
      "tasks": {
        "be": [
          {"id": "t1", "status": "running", "issue_id": "i1", "created_at": iso(-600)},
          {"id": "t6", "status": "failed", "failure_reason": "auth", "issue_id": "i6", "created_at": iso(-3600), "completed_at": iso(-3500)},
          {"id": "t7", "status": "cancelled", "cancelled_by": {"type": "member", "id": "u1"}, "issue_id": "i7", "created_at": iso(-3600)},
          {"id": "t8", "status": "completed", "issue_id": "i8", "created_at": iso(-7200), "completed_at": iso(-7000)},
        ],
        "qa": [], "mk": [
          {"id": "tm", "status": "completed", "issue_id": "m1", "created_at": iso(-900), "completed_at": iso(-890)},
        ]},
      "issues": [
        issue("i1", "X-1", "busy", "in_progress", "high", "be", iso(-600)),
        issue("i2", "X-2", "todo high", "todo", "high", "be", iso(-5000)),
        issue("i3", "X-3", "todo low", "todo", "low", "be", iso(-9000)),
        issue("i4", "X-4", "question", "question_1", "urgent", "be", iso(-9000), "Question", "blocked"),
        issue("i5", "X-5", "backlog", "backlog", "urgent", "be", iso(-9000)),
        issue("i6", "X-6", "failed run", "in_progress", "medium", "be", iso(-3500)),
        issue("i7", "X-7", "human cancelled", "in_progress", "high", "be", iso(-3600)),
        issue("i8", "X-8", "completed then human comment", "in_progress", "urgent", "be", iso(-60), activity=iso(-60)),
        issue("m1", "X-20", "Mika parent waiting children", "in_progress", "urgent", "mk", iso(-880)),
        issue("r1", "X-11", "review old", "in_review", "medium", "qa", iso(-90000)),
        issue("r2", "X-12", "review mid", "in_review", "medium", "qa", iso(-80000)),
        issue("r3", "X-13", "review new", "in_review", "medium", "qa", iso(-70000)),
        issue("t9", "X-9", "test apres deploiement", "test_1", "high", "qa", iso(-99999), "Test après déploiement", "in_review"),
      ],
    }

STATE = {}
reset()
LOCK = threading.Lock()
WS_CLIENTS = set()
LOOP = None

class H(BaseHTTPRequestHandler):
    def _send(self, obj, code=200):
        b = json.dumps(obj).encode(); self.send_response(code)
        self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)
    def _ws(self):
        """Workspace selected by the request, or None when the token is wrong."""
        if self.headers.get("Authorization") != "Bearer tok":
            return None
        return STATE["ws"].get(self.headers.get("X-Workspace-ID", ""), {})
    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path); q = parse_qs(u.query)
        W = self._ws()
        if W is None: return self._send({"error": "unauthorized"}, 401)
        with LOCK:
            if u.path == "/api/workspaces":
                return self._send([w["meta"] for w in STATE["ws"].values()])
            if not W: return self._send({"error": "workspace required"}, 400)
            if u.path == "/api/agents": return self._send({"agents": W["agents"]})
            if u.path.startswith("/api/agents/") and u.path.endswith("/tasks"):
                aid = u.path.split("/")[3]
                if aid not in W["tasks"]: return self._send({"error": "agent not in workspace"}, 404)
                return self._send(W["tasks"][aid])
            if u.path == "/api/issues":
                iss = [i for i in W["issues"] if i["status"] not in ("done", "cancelled")]
                if "assignee_id" in q: iss = [i for i in iss if i["assignee_id"] == q["assignee_id"][0]]
                return self._send({"issues": iss, "total": len(iss)})
        self._send({"error": "not found"}, 404)
    def do_POST(self):
        W = self._ws()
        if not W: return self._send({"error": "unauthorized"}, 401)
        n = int(self.headers.get("Content-Length") or 0); self.rfile.read(n)
        parts = self.path.split("/")
        if len(parts) == 5 and parts[2] == "issues" and parts[4] == "rerun":
            with LOCK:
                iss = next((i for i in W["issues"] if i["id"] == parts[3]), None)
                if iss is None:   # issue of another workspace: refused, like the real server
                    STATE.setdefault("cross_workspace", []).append(parts[3])
                    return self._send({"error": "issue not found"}, 404)
                STATE["reruns"].append((W["meta"]["id"], iss["assignee_id"], iss["identifier"], time.time()))
                W["tasks"][iss["assignee_id"]].append({"id": f"t{len(STATE['reruns'])+100}", "status": "queued", "issue_id": iss["id"]})
            return self._send({"id": "newtask"})
        self._send({"error": "not found"}, 404)
    def log_message(self, *a): pass

async def ws_handler(ws):
    from urllib.parse import urlparse, parse_qs
    wid = parse_qs(urlparse(ws.request.path).query).get("workspace_id", [""])[0]
    first = json.loads(await ws.recv())
    if first.get("type") != "auth" or first["payload"]["token"] != "tok" or wid not in STATE["ws"]:
        await ws.send(json.dumps({"error": "bad auth"})); return
    entry = (wid, ws)
    WS_CLIENTS.add(entry)
    try: await ws.wait_closed()
    finally: WS_CLIENTS.discard(entry)

def connected_workspaces():
    return sorted({w for w, _ in list(WS_CLIENTS)})

def emit(msg, workspace="ws1"):
    """Broadcast an event to the clients of one workspace only."""
    async def _send():
        for w, c in list(WS_CLIENTS):
            if w == workspace: await c.send(json.dumps(msg))
    asyncio.run_coroutine_threadsafe(_send(), LOOP)

def start(http_port=8765, ws_port=8766):
    global LOOP
    threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", http_port), H).serve_forever, daemon=True).start()
    LOOP = asyncio.new_event_loop()
    async def run():
        async with websockets.serve(ws_handler, "127.0.0.1", ws_port):
            await asyncio.Future()
    threading.Thread(target=lambda: LOOP.run_until_complete(run()), daemon=True).start()
    time.sleep(0.5)
