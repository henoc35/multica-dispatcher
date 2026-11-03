#!/usr/bin/env python3
"""
multica-dispatcher — keeps Multica agents working non-stop, across several workspaces.

Multi-workspace: MULTICA_WORKSPACES=all (default) handles every workspace the token can
access, re-discovered at each reconcile pass; or a comma-separated list of ids / slugs.
Each workspace has its own realtime connection, agents and review-priority mode.

Event-driven: listens to the Multica realtime WebSocket (task:completed / failed /
cancelled, issue:updated). As soon as an agent frees a slot, it starts that agent's
next eligible issue via POST /api/issues/{id}/rerun. A light reconcile pass every
RECONCILE_SECONDS catches any missed event. No LLM involved: zero token cost.

Eligible issue for an agent:
  - assigned to the agent, open, not in triage;
  - status todo / in_progress (custom statuses: status_category todo / in_progress),
    plus in_review for REVIEWER_AGENTS;
  - status_name not matching EXCLUDE_STATUS_REGEX (e.g. "Question", "Test après déploiement");
  - no queued / running run already on it for this agent.
Order: priority (urgent > high > medium > low > none), then oldest update first.
Review-priority mode: when more than REVIEW_PRIORITY_LIMIT issues are in review,
non-reviewer agents only resume in_progress work (no new todo starts).

Safety guards: DRY_RUN, per-agent hourly cap, per-issue cooldown, no-progress parking,
failure circuit breaker (e.g. expired provider login), single in-flight dispatch per agent.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

import websockets


# ---------------------------------------------------------------- configuration
def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def load_cli_config() -> dict:
    path = Path(env("MULTICA_CONFIG", str(Path.home() / ".multica" / "config.json")))
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


_cli = load_cli_config()
SERVER_URL = (env("MULTICA_SERVER_URL") or _cli.get("server_url", "")).rstrip("/")
# Workspaces: "all" (default), or ids / slugs separated by commas. MULTICA_WORKSPACE_ID
# (single workspace, older setting) is still honoured when MULTICA_WORKSPACES is unset.
WORKSPACES = env("MULTICA_WORKSPACES") or env("MULTICA_WORKSPACE_ID") or "all"
EXCLUDE_WORKSPACES = {w.strip() for w in env("EXCLUDE_WORKSPACES").split(",") if w.strip()}
TOKEN = env("MULTICA_TOKEN") or _cli.get("token", "")
WS_URL = env("MULTICA_WS_URL")  # optional; default: SERVER_URL with ws(s):// + /ws

DRY_RUN = env("DRY_RUN", "1") == "1"
AGENTS = {a.strip() for a in env("AGENTS").split(",") if a.strip()}  # empty = all agents
REVIEWER_AGENTS = {a.strip() for a in env("REVIEWER_AGENTS", "qa-lead,security-lead,seo-lead").split(",") if a.strip()}
REVIEW_PRIORITY_LIMIT = env_int("REVIEW_PRIORITY_LIMIT", 10)
EXCLUDE_STATUS_REGEX = re.compile(env("EXCLUDE_STATUS_REGEX", "question|test"), re.IGNORECASE)
RECONCILE_SECONDS = env_int("RECONCILE_SECONDS", 60)
DEBOUNCE_SECONDS = env_int("DEBOUNCE_SECONDS", 3)
MAX_DISPATCH_PER_AGENT_PER_HOUR = env_int("MAX_DISPATCH_PER_AGENT_PER_HOUR", 20)
ISSUE_COOLDOWN_SECONDS = env_int("ISSUE_COOLDOWN_SECONDS", 300)
NO_PROGRESS_LIMIT = env_int("NO_PROGRESS_LIMIT", 3)
MAX_DISPATCH_PER_ISSUE_PER_DAY = env_int("MAX_DISPATCH_PER_ISSUE_PER_DAY", 8)
FAILURE_BREAKER_COUNT = env_int("FAILURE_BREAKER_COUNT", 3)
FAILURE_BREAKER_WINDOW = env_int("FAILURE_BREAKER_WINDOW", 600)
FAILURE_BREAKER_PAUSE = env_int("FAILURE_BREAKER_PAUSE", 900)
STATE_FILE = Path(env("STATE_FILE", "/data/state.json"))
HEALTH_PORT = env_int("HEALTH_PORT", 8080)
ISSUE_PAGE_LIMIT = env_int("ISSUE_PAGE_LIMIT", 200)
ACTIVITY_GRACE_SECONDS = env_int("ACTIVITY_GRACE_SECONDS", 120)

ACTIVE_TASK = {"dispatched", "running", "waiting_local_directory"}
PENDING_TASK = {"queued", "deferred"}
PRIORITY_RANK = {"urgent": 0, "high": 1, "medium": 2, "low": 3, "none": 4, "": 4}

logging.basicConfig(level=env("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dispatcher")


# ---------------------------------------------------------------- HTTP client
class ApiError(Exception):
    pass


def _request(method: str, path: str, workspace_id: str = "", body: dict | None = None, query: dict | None = None):
    url = SERVER_URL + path
    if query:
        url += "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v not in (None, "")})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    if workspace_id:
        req.add_header("X-Workspace-ID", workspace_id)
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "multica-dispatcher/1.0")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        raise ApiError(f"{method} {path} -> {e.code}: {detail}") from e
    except urllib.error.URLError as e:
        raise ApiError(f"{method} {path} -> {e.reason}") from e


async def api(method: str, path: str, workspace_id: str = "", body: dict | None = None, query: dict | None = None):
    return await asyncio.to_thread(_request, method, path, workspace_id, body, query)


def objects_with(data, *keys: str) -> list[dict]:
    """Extract every dict carrying all `keys`, whatever the envelope ({"issues": [...]}, list, ...)."""
    found: list[dict] = []

    def walk(x):
        if isinstance(x, dict):
            if all(k in x for k in keys):
                found.append(x)
                return
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(data)
    return found


def ts(value) -> float:
    """ISO-8601 timestamp -> epoch seconds (0 when missing or invalid)."""
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
    except ValueError:
        return 0.0


def last_run_verdict(issue: dict, last_task: dict | None) -> tuple[bool, str]:
    """Should this issue be (re)started, given the agent's latest run on it?
    - never run, or the last run failed / was cancelled by the system: yes (resume after a
      crash, an expired login, a sleeping machine...);
    - last run cancelled by a person: no, respect that decision;
    - last run completed normally: only if something happened on the issue afterwards
      (a comment, a status change...), otherwise the agent is waiting for an event."""
    if last_task is None:
        return True, "jamais lancée"
    status = last_task.get("status")
    if status == "failed":
        return True, f"reprise après échec ({last_task.get('failure_reason') or 'inconnu'})"
    if status == "cancelled":
        actor = last_task.get("cancelled_by") or {}
        if isinstance(actor, dict) and actor.get("type") in ("member", "user"):
            return False, "dernière exécution annulée par un humain"
        return True, "reprise après annulation automatique"
    if status == "completed":
        done = ts(last_task.get("completed_at"))
        activity = max(ts(issue.get("last_activity_at")), ts(issue.get("updated_at")))
        if done and activity > done + ACTIVITY_GRACE_SECONDS:
            return True, "activité depuis la dernière exécution"
        return False, "attend un événement (rien de nouveau depuis la dernière exécution)"
    return False, f"exécution {status}"


# ---------------------------------------------------------------- persistent state
class State:
    """Per-issue dispatch history (cooldown, no-progress parking), persisted to disk."""

    def __init__(self, path: Path):
        self.path = path
        self.issues: dict[str, dict] = {}
        try:
            self.issues = json.loads(path.read_text()).get("issues", {})
        except (OSError, ValueError):
            pass

    def save(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"issues": self.issues}, indent=1))
            tmp.replace(self.path)
        except OSError as e:
            log.warning("state not saved: %s", e)

    def entry(self, issue_id: str) -> dict:
        e = self.issues.setdefault(issue_id, {})
        e.setdefault("last_dispatch", 0)
        e.setdefault("signature", "")
        e.setdefault("no_progress", 0)
        e.setdefault("day_dispatches", [])
        return e


# ---------------------------------------------------------------- workspace selection
def agent_followed(ws: dict, agent_name: str) -> bool:
    """AGENTS entries: "qa-lead" (any workspace) or "<slug>/qa-lead" (one workspace only)."""
    if not AGENTS:
        return True
    return agent_name in AGENTS or f'{ws.get("slug", "")}/{agent_name}' in AGENTS or f'{ws["id"]}/{agent_name}' in AGENTS


def select_workspaces(listed: list[dict]) -> list[dict]:
    """Apply MULTICA_WORKSPACES / EXCLUDE_WORKSPACES to the workspaces the token can see."""
    def excluded(w):
        return w["id"] in EXCLUDE_WORKSPACES or w.get("slug", "") in EXCLUDE_WORKSPACES
    wanted = [w.strip() for w in WORKSPACES.split(",") if w.strip()]
    if not wanted or wanted == ["all"]:
        return [w for w in listed if not excluded(w)]
    by_key = {}
    for w in listed:
        by_key[w["id"]] = w
        if w.get("slug"):
            by_key[w["slug"]] = w
    chosen, seen = [], set()
    for key in wanted:
        w = by_key.get(key)
        if w is None:
            if not listed:                      # listing unavailable: trust an explicit id
                w = {"id": key, "slug": key, "name": key}
            else:
                log.warning("workspace « %s » introuvable ou inaccessible avec ce token", key)
                continue
        if w["id"] not in seen and not excluded(w):
            seen.add(w["id"])
            chosen.append(w)
    return chosen


# ---------------------------------------------------------------- per-workspace dispatcher
class WorkspaceDispatcher:
    """Everything that is scoped to one Multica workspace: agents, review mode, realtime feed."""

    def __init__(self, ws: dict, state: "State"):
        self.ws = ws
        self.id = ws["id"]
        self.label = ws.get("slug") or ws.get("name") or ws["id"]
        self.state = state
        self.agents: dict[str, dict] = {}          # id -> agent
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.pending: dict[str, asyncio.TimerHandle] = {}
        self.dispatch_log: dict[str, deque] = defaultdict(deque)    # agent_id -> timestamps
        self.failures: dict[str, deque] = defaultdict(deque)        # agent_id -> failure timestamps
        self.paused_until: dict[str, float] = {}
        self.review_count = 0
        self.sim_last: dict[str, float] = {}       # DRY_RUN only: issue_id -> simulated launch time
        self.simulated: deque = deque(maxlen=50)   # DRY_RUN only: latest simulated launches
        self.ws_connected = False
        self.listener: asyncio.Task | None = None

    async def call(self, method: str, path: str, body: dict | None = None, query: dict | None = None):
        return await api(method, path, self.id, body, query)

    # ---------- data loading
    async def refresh_agents(self):
        data = await self.call("GET", "/api/agents")
        agents = {a["id"]: a for a in objects_with(data, "id", "name")
                  if not a.get("archived_at") and agent_followed(self.ws, a["name"])}
        if set(agents) != set(self.agents):
            log.info("[%s] agents suivis : %s", self.label,
                     ", ".join(sorted(a["name"] for a in agents.values())) or "(aucun)")
        self.agents = agents

    async def open_issues(self, assignee_id: str | None = None) -> list[dict]:
        q = {"open_only": "true", "limit": ISSUE_PAGE_LIMIT}
        if assignee_id:
            q.update({"assignee_id": assignee_id, "assignee_types": "agent"})
        data = await self.call("GET", "/api/issues", query=q)
        issues = objects_with(data, "id", "identifier", "status")
        if assignee_id:  # guard: never trust a filter we cannot verify
            issues = [i for i in issues if i.get("assignee_id") == assignee_id]
        return issues

    async def refresh_review_count(self):
        try:
            issues = await self.open_issues()
            self.review_count = sum(1 for i in issues if i.get("status") == "in_review")
        except ApiError as e:
            log.warning("[%s] comptage des relectures impossible : %s", self.label, e)

    # ---------- eligibility
    def eligible(self, issue: dict, agent: dict) -> tuple[bool, str]:
        status = issue.get("status", "")
        category = issue.get("status_category", "") or status
        name = issue.get("status_name", "") or ""
        if issue.get("triage_state"):
            return False, "en triage"
        if name and EXCLUDE_STATUS_REGEX.search(name):
            return False, f"colonne exclue ({name})"
        allowed = {"todo", "in_progress"}
        if agent["name"] in REVIEWER_AGENTS:
            allowed.add("in_review")
        if category not in allowed and status not in allowed:
            return False, f"statut {name or status}"
        if (self.review_count > REVIEW_PRIORITY_LIMIT and agent["name"] not in REVIEWER_AGENTS
                and (status == "todo" or category == "todo")):
            return False, "mode relecture prioritaire"
        return True, ""

    # ---------- guards
    def hourly_ok(self, agent_id: str) -> bool:
        q, now = self.dispatch_log[agent_id], time.time()
        while q and now - q[0] > 3600:
            q.popleft()
        return len(q) < MAX_DISPATCH_PER_AGENT_PER_HOUR

    @staticmethod
    def signature(issue: dict) -> str:
        """Real progress = status, assignee or issue revision changed. Comments alone do not
        count: a run that did nothing still leaves a closing comment."""
        return f'{issue.get("status")}|{issue.get("assignee_id")}|{issue.get("revision")}'

    def issue_ok(self, issue: dict) -> tuple[bool, str]:
        now = time.time()
        if DRY_RUN and now - self.sim_last.get(issue["id"], 0) < ISSUE_COOLDOWN_SECONDS:
            return False, "délai entre deux relances"
        if DRY_RUN:   # read-only view of the real history: the simulation never writes to it
            e = {"last_dispatch": 0, "signature": "", "no_progress": 0, "day_dispatches": [],
                 **self.state.issues.get(issue["id"], {})}
        else:
            e = self.state.entry(issue["id"])
        e["day_dispatches"] = [t for t in e["day_dispatches"] if now - t < 86400]
        if e["signature"] and self.signature(issue) != e["signature"]:
            e["no_progress"] = 0                      # the issue moved: progress made
        if e["no_progress"] >= NO_PROGRESS_LIMIT:
            return False, (f"mise de côté : {NO_PROGRESS_LIMIT} relances sans changement de statut "
                           f"(reprend dès que l'issue change)")
        if len(e["day_dispatches"]) >= MAX_DISPATCH_PER_ISSUE_PER_DAY:
            return False, f"mise de côté : plafond de {MAX_DISPATCH_PER_ISSUE_PER_DAY} relances/jour"
        if now - e["last_dispatch"] < ISSUE_COOLDOWN_SECONDS:
            return False, "délai entre deux relances"
        return True, ""

    def record_dispatch(self, agent_id: str, issue: dict):
        if DRY_RUN:
            # Simulation: nothing really ran, so the issue cannot have moved. Only the cooldown
            # is kept (in memory) to avoid repeating the same line every pass; no-progress
            # parking, daily caps and the state file are left untouched, so switching to
            # DRY_RUN=0 starts from a clean history.
            self.sim_last[issue["id"]] = time.time()
            return
        e = self.state.entry(issue["id"])
        sig = self.signature(issue)
        e["no_progress"] = e["no_progress"] + 1 if e["last_dispatch"] and e["signature"] == sig else 0
        e["last_dispatch"] = time.time()
        e["signature"] = sig
        e["workspace"] = self.id
        e["identifier"] = issue.get("identifier", "")
        e["day_dispatches"].append(time.time())
        self.dispatch_log[agent_id].append(time.time())
        self.state.save()

    def note_failure(self, agent_id: str):
        q, now = self.failures[agent_id], time.time()
        q.append(now)
        while q and now - q[0] > FAILURE_BREAKER_WINDOW:
            q.popleft()
        if len(q) >= FAILURE_BREAKER_COUNT:
            self.paused_until[agent_id] = now + FAILURE_BREAKER_PAUSE
            q.clear()
            name = self.agents.get(agent_id, {}).get("name", agent_id)
            log.error("[%s] %s : %d échecs en %ds, relances suspendues %ds (vérifie la connexion Claude / les limites)",
                      self.label, name, FAILURE_BREAKER_COUNT, FAILURE_BREAKER_WINDOW, FAILURE_BREAKER_PAUSE)

    # ---------- core
    async def dispatch_agent(self, agent_id: str, reason: str):
        agent = self.agents.get(agent_id)
        if not agent:
            return
        if time.time() < self.paused_until.get(agent_id, 0):
            return
        async with self.locks[agent_id]:
            try:
                tasks = objects_with(await self.call("GET", f"/api/agents/{agent_id}/tasks", query={"limit": 100}), "id", "status")
                issues = await self.open_issues(agent_id)
            except ApiError as e:
                log.warning("[%s] %s : lecture impossible (%s)", self.label, agent["name"], e)
                return
            active = sum(1 for t in tasks if t["status"] in ACTIVE_TASK)
            queued = sum(1 for t in tasks if t["status"] in PENDING_TASK)
            busy_issues = {t.get("issue_id") for t in tasks if t["status"] in ACTIVE_TASK | PENDING_TASK}
            latest: dict[str, dict] = {}
            for t in tasks:
                iid = t.get("issue_id")
                if iid and ts(t.get("created_at")) >= ts(latest.get(iid, {}).get("created_at")):
                    latest[iid] = t
            capacity = int(agent.get("max_concurrent_tasks") or 1)
            free = capacity - active - queued
            if free <= 0:
                log.debug("[%s] %s : complet (%d/%d actives, %d en file)", self.label, agent["name"], active, capacity, queued)
                return

            candidates = []
            for issue in issues:
                if issue["id"] in busy_issues:
                    continue
                ok, why = self.eligible(issue, agent)
                if ok:
                    ok, why = last_run_verdict(issue, latest.get(issue["id"]))
                    issue["_why"] = why
                if ok:
                    ok, why = self.issue_ok(issue)
                if ok:
                    candidates.append(issue)
                elif why.startswith("mise de côté"):
                    log.info("[%s] %s %s : %s", self.label, agent["name"], issue["identifier"], why)
            candidates.sort(key=lambda i: (PRIORITY_RANK.get(i.get("priority", ""), 4), i.get("updated_at", "")))

            for issue in candidates[:free]:
                if not self.hourly_ok(agent_id):
                    log.warning("[%s] %s : plafond de %d relances/heure atteint", self.label, agent["name"], MAX_DISPATCH_PER_AGENT_PER_HOUR)
                    break
                label = (f"[{self.label}] {agent['name']} → {issue['identifier']} « {issue.get('title', '')[:60]} » "
                         f"({reason} ; {issue.get('_why', '')})")
                if DRY_RUN:
                    log.info("[simulation] lancerait %s", label)
                    self.simulated.append({"at": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                                           "agent": agent["name"], "issue": issue["identifier"],
                                           "reason": f"{reason} ; {issue.get('_why', '')}"})
                    self.record_dispatch(agent_id, issue)
                    continue
                try:
                    # Last-moment check: rerun cancels any run already on the issue. If Multica
                    # itself started one in the meantime (mention, assignment...), leave it alone.
                    fresh = objects_with(await self.call("GET", f"/api/agents/{agent_id}/tasks", query={"limit": 100}), "id", "status")
                    if any(t.get("issue_id") == issue["id"] and t["status"] in ACTIVE_TASK | PENDING_TASK for t in fresh):
                        log.info("[%s] %s %s : déjà démarrée entre-temps, rien à faire", self.label, agent["name"], issue["identifier"])
                        continue
                    await self.call("POST", f"/api/issues/{issue['id']}/rerun", body={})
                    log.info("lancé %s", label)
                    self.record_dispatch(agent_id, issue)
                except ApiError as e:
                    log.warning("échec du lancement %s : %s", label, e)
                    self.note_failure(agent_id)

    def schedule(self, agent_id: str, reason: str):
        """Debounced dispatch: several events in a burst produce a single pass."""
        if agent_id not in self.agents:
            return
        loop = asyncio.get_running_loop()
        if agent_id in self.pending:
            self.pending[agent_id].cancel()
        self.pending[agent_id] = loop.call_later(
            DEBOUNCE_SECONDS, lambda: asyncio.ensure_future(self.dispatch_agent(agent_id, reason)))

    async def reconcile_once(self):
        await self.refresh_agents()
        await self.refresh_review_count()
        for agent_id in list(self.agents):
            await self.dispatch_agent(agent_id, "rattrapage")

    # ---------- realtime events
    def handle_event(self, msg: dict):
        etype, payload = msg.get("type", ""), msg.get("payload") or {}
        if etype in ("task:completed", "task:cancelled", "task:failed"):
            agent_id = payload.get("agent_id", "")
            if etype == "task:failed":
                if payload.get("retry_pending"):
                    return                                   # Multica retries on its own
                self.note_failure(agent_id)
            self.schedule(agent_id, etype)
        elif etype == "issue:updated" and (payload.get("status_changed") or payload.get("assignee_changed")):
            issue = payload.get("issue") or {}
            if issue.get("status") == "in_review" or payload.get("prev_status") == "in_review":
                asyncio.ensure_future(self.refresh_review_count())
            if issue.get("assignee_type") == "agent" and issue.get("assignee_id"):
                self.schedule(issue["assignee_id"], "issue modifiée")

    async def listen(self):
        base = WS_URL or (re.sub(r"^http", "ws", SERVER_URL) + "/ws")
        ws_url = base + "?" + urllib.parse.urlencode({"workspace_id": self.id})
        backoff = 2
        while True:
            try:
                async with websockets.connect(ws_url, max_size=4 * 1024 * 1024, ping_interval=30,
                                              user_agent_header="multica-dispatcher/1.0") as ws:
                    await ws.send(json.dumps({"type": "auth", "payload": {"token": TOKEN}}))
                    self.ws_connected = True
                    backoff = 2
                    log.info("[%s] temps réel connecté", self.label)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        if isinstance(msg, dict) and msg.get("error"):
                            log.error("[%s] temps réel refusé : %s", self.label, msg["error"])
                            break
                        self.handle_event(msg)
            except asyncio.CancelledError:
                self.ws_connected = False
                raise
            except Exception as e:
                log.warning("[%s] temps réel déconnecté (%s), nouvelle tentative dans %ds", self.label, e, backoff)
            self.ws_connected = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    def stop(self):
        for h in self.pending.values():
            h.cancel()
        if self.listener:
            self.listener.cancel()

    def status(self) -> dict:
        now = time.time()
        return {
            "id": self.id,
            "name": self.ws.get("name", ""),
            "realtime_connected": self.ws_connected,
            "review_count": self.review_count,
            "review_priority_mode": self.review_count > REVIEW_PRIORITY_LIMIT,
            "agents": {a["name"]: {
                "dispatches_last_hour": len([t for t in self.dispatch_log[i] if now - t < 3600]),
                "paused_for_s": max(0, int(self.paused_until.get(i, 0) - now)),
            } for i, a in self.agents.items()},
            **({"simulated_launches": list(self.simulated)} if DRY_RUN else {}),
            "parked_issues": sorted(v.get("identifier") or k for k, v in self.state.issues.items()
                                    if v.get("workspace") == self.id and v.get("no_progress", 0) >= NO_PROGRESS_LIMIT),
        }


# ---------------------------------------------------------------- supervisor
class Dispatcher:
    """Discovers the workspaces to handle and runs one WorkspaceDispatcher per workspace."""

    def __init__(self):
        self.state = State(STATE_FILE)
        self.workspaces: dict[str, WorkspaceDispatcher] = {}
        self.started = time.time()

    async def refresh_workspaces(self):
        try:
            listed = objects_with(await api("GET", "/api/workspaces"), "id", "name")
        except ApiError as e:
            if self.workspaces:
                log.warning("liste des workspaces indisponible (%s), on garde les %d actuels", e, len(self.workspaces))
                return
            log.warning("liste des workspaces indisponible (%s)", e)
            listed = []
        chosen = {w["id"]: w for w in select_workspaces(listed)}
        for wid in list(self.workspaces):
            if wid not in chosen:
                log.info("[%s] workspace retiré, arrêt de son suivi", self.workspaces[wid].label)
                self.workspaces.pop(wid).stop()
        for wid, w in chosen.items():
            if wid in self.workspaces:
                self.workspaces[wid].ws = w
                continue
            wd = WorkspaceDispatcher(w, self.state)
            wd.listener = asyncio.create_task(wd.listen())
            self.workspaces[wid] = wd
            log.info("[%s] workspace suivi (%s)", wd.label, wid)
        if not self.workspaces:
            log.warning("aucun workspace à suivre (vérifie MULTICA_WORKSPACES et les droits du token)")

    async def reconcile(self):
        while True:
            try:
                await self.refresh_workspaces()
                for wd in list(self.workspaces.values()):
                    try:
                        await wd.reconcile_once()
                    except ApiError as e:
                        log.warning("[%s] rattrapage : %s", wd.label, e)
            except Exception:  # never let the safety net die
                log.exception("rattrapage : erreur inattendue")
            await asyncio.sleep(RECONCILE_SECONDS)

    def stop(self):
        for wd in self.workspaces.values():
            wd.stop()

    def status(self) -> dict:
        return {
            "dry_run": DRY_RUN,
            "uptime_s": int(time.time() - self.started),
            "workspaces": {wd.label: wd.status() for wd in self.workspaces.values()},
        }


def serve_health(d: Dispatcher):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps(d.status(), ensure_ascii=False, indent=1).encode() if self.path.startswith("/status") else b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "application/json" if self.path.startswith("/status") else "text/plain")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    Thread(target=HTTPServer(("0.0.0.0", HEALTH_PORT), H).serve_forever, daemon=True).start()


async def main():
    missing = [n for n, v in (("MULTICA_SERVER_URL", SERVER_URL), ("MULTICA_TOKEN", TOKEN)) if not v]
    if missing:
        raise SystemExit(f"Configuration manquante : {', '.join(missing)}")
    d = Dispatcher()
    serve_health(d)
    log.info("démarrage — %s, workspaces : %s, rattrapage toutes les %ds",
             "SIMULATION (DRY_RUN=1)" if DRY_RUN else "ACTIF", WORKSPACES, RECONCILE_SECONDS)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    task = asyncio.create_task(d.reconcile())
    await stop.wait()
    task.cancel()
    d.stop()
    d.state.save()
    log.info("arrêt propre")


if __name__ == "__main__":
    asyncio.run(main())
