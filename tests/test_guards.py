"""Unit tests for the dispatcher's safety guards."""
import importlib, os, sys, tempfile, time
os.environ.update(MULTICA_SERVER_URL="http://x", MULTICA_WORKSPACE_ID="w", MULTICA_TOKEN="t", DRY_RUN="0",
                  STATE_FILE=tempfile.mktemp(suffix=".json"), NO_PROGRESS_LIMIT="3",
                  MAX_DISPATCH_PER_ISSUE_PER_DAY="5", ISSUE_COOLDOWN_SECONDS="0",
                  FAILURE_BREAKER_COUNT="3", FAILURE_BREAKER_PAUSE="900")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
d = importlib.import_module("dispatcher")
ok = True
def check(name, cond):
    global ok; ok &= bool(cond); print(("PASS " if cond else "FAIL ") + name)

D = d.WorkspaceDispatcher({"id": "w", "slug": "alpha"}, d.State(d.STATE_FILE))
issue = {"id": "i1", "status": "in_progress", "assignee_id": "a", "revision": 1}
for _ in range(4):
    assert D.issue_ok(issue)[0]; D.record_dispatch("a", issue)
check("1. après 3 relances sans changement d'état : mise de côté", not D.issue_ok(issue)[0])
issue2 = dict(issue, status="in_review")
check("2. l'issue change de statut : elle redevient éligible", D.issue_ok(issue2)[0])

other = {"id": "i2", "status": "todo", "assignee_id": "a", "revision": 1}
for n in range(5):
    other["revision"] = n            # progresses each time, so only the daily cap can stop it
    assert D.issue_ok(other)[0]; D.record_dispatch("a", other)
other["revision"] = 99
check("3. plafond de 5 relances par jour et par issue", not D.issue_ok(other)[0])

D.agents = {"a": {"id": "a", "name": "backend-lead"}}
for _ in range(3):
    D.note_failure("a")
check("4. 3 échecs rapprochés : relances de l'agent suspendues", D.paused_until.get("a", 0) > time.time())

check("5. parsing des dates ISO (Z et décalage)", d.ts("2026-11-03T10:00:00Z") == d.ts("2026-11-03T11:00:00+01:00") > 0)
check("6. pas de relance si annulée par un humain", not d.last_run_verdict({}, {"status": "cancelled", "cancelled_by": {"type": "member"}})[0])
check("7. relance si annulée par le système", d.last_run_verdict({}, {"status": "cancelled", "cancelled_by": {"type": "system"}})[0])

# workspace selection
d.WORKSPACES, d.EXCLUDE_WORKSPACES = "all", {"archive"}
listed = [{"id": "w1", "slug": "alpha"}, {"id": "w2", "slug": "beta"}, {"id": "w3", "slug": "archive"}]
check("8. « all » : tous les workspaces sauf les exclus", [w["id"] for w in d.select_workspaces(listed)] == ["w1", "w2"])
d.WORKSPACES, d.EXCLUDE_WORKSPACES = "beta, w1,inconnu", set()
check("9. liste explicite par slug ou id, inconnus ignorés", [w["id"] for w in d.select_workspaces(listed)] == ["w2", "w1"])
d.WORKSPACES = "w9"
check("10. liste indisponible : un id explicite est utilisé tel quel", [w["id"] for w in d.select_workspaces([])] == ["w9"])
d.AGENTS = {"qa-lead", "beta/Mika"}
check("11. filtre d'agents global et par workspace",
      d.agent_followed({"id": "w1", "slug": "alpha"}, "qa-lead") and d.agent_followed({"id": "w2", "slug": "beta"}, "Mika")
      and not d.agent_followed({"id": "w1", "slug": "alpha"}, "Mika"))

# simulation must not poison the real history
d.DRY_RUN = True
S = d.WorkspaceDispatcher({"id": "w", "slug": "alpha"}, d.State(d.STATE_FILE))
d.ISSUE_COOLDOWN_SECONDS = 300
sim = {"id": "s1", "status": "todo", "assignee_id": "a", "revision": 1}
S.record_dispatch("a", sim)
check("12. simulation : délai respecté (pas la même ligne à chaque passage)", not S.issue_ok(sim)[0])
d.ISSUE_COOLDOWN_SECONDS = 0
for _ in range(10):
    S.record_dispatch("a", sim)
check("13. simulation : jamais de mise de côté ni de plafond journalier", S.issue_ok(sim)[0] and "s1" not in S.state.issues)
d.DRY_RUN = False

# state written by an older version (no format version): discarded
import json as _json, pathlib as _pl
old = _pl.Path(tempfile.mktemp(suffix=".json"))
old.write_text(_json.dumps({"issues": {"x": {"no_progress": 9, "last_dispatch": 1, "signature": "s", "day_dispatches": []}}}))
check("14. historique d'une ancienne version (sans numéro) : ignoré", d.State(old).issues == {})
new = _pl.Path(tempfile.mktemp(suffix=".json"))
st = d.State(new); st.entry("y"); st.save()
check("15. historique au format actuel : conservé", "y" in d.State(new).issues)
sys.exit(0 if ok else 1)
