"""End-to-end tests: dispatcher against the fake Multica server."""
import os, subprocess, sys, time, json, urllib.request, tempfile
sys.path.insert(0, os.path.dirname(__file__))
import fake_multica as fm

fm.start()
ok = True

def check(name, cond):
    global ok
    ok &= bool(cond)
    print(("PASS " if cond else "FAIL ") + name)

def reruns(workspace="ws1"):
    with fm.LOCK:
        return [(a, i) for w, a, i, _ in fm.STATE["reruns"] if w == workspace]

def status():
    return json.loads(urllib.request.urlopen("http://127.0.0.1:8799/status").read())

def run(extra_env, seconds, during=None):
    env = dict(os.environ, MULTICA_SERVER_URL="http://127.0.0.1:8765", MULTICA_WS_URL="ws://127.0.0.1:8766/ws",
               MULTICA_TOKEN="tok", DRY_RUN="0", RECONCILE_SECONDS="3600",
               DEBOUNCE_SECONDS="1", ISSUE_COOLDOWN_SECONDS="0", STATE_FILE=tempfile.mktemp(suffix=".json"),
               HEALTH_PORT="8799", LOG_LEVEL="INFO")
    env.update(extra_env)
    p = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(__file__), "..", "dispatcher.py")],
                         env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        time.sleep(seconds)
        if during:
            during()
    finally:
        p.terminate()
        return p.communicate(timeout=5)[0]

# Scenarios 1-3 use the older single-workspace setting (MULTICA_WORKSPACE_ID) on purpose:
# it must keep working unchanged.
SINGLE = {"MULTICA_WORKSPACE_ID": "ws1"}

# ---------- scenario 1: normal mode
def scenario1():
    r = reruns()
    check("1. 3 places libres -> X-8 (urgent, activité), X-2 (haute), X-6 (échec) dans cet ordre", r[:3] == [("be", "X-8"), ("be", "X-2"), ("be", "X-6")])
    check("2. annulée par un humain (X-7) : jamais relancée", ("be", "X-7") not in r)
    check("3. colonne Question (X-4) et backlog (X-5) : jamais lancés", ("be", "X-4") not in r and ("be", "X-5") not in r)
    check("4. Mika terminé sans rien de nouveau (X-20) : pas relancé (il attend)", ("mk", "X-20") not in r)
    check("5. qa-lead : les 2 relectures les plus anciennes", ("qa", "X-11") in r and ("qa", "X-12") in r and ("qa", "X-13") not in r)
    check("6. « Test après déploiement » (X-9) : jamais lancé", ("qa", "X-9") not in r)
    with fm.LOCK:
        fm.STATE["ws"]["ws1"]["tasks"]["be"][0].update(status="completed", completed_at=fm.iso())
    fm.emit({"type": "task:completed", "payload": {"task_id": "t1", "agent_id": "be", "issue_id": "i1", "status": "completed"}})
    time.sleep(3)
    r = reruns()
    check("7. task:completed -> place libérée -> X-3 lancé tout de suite", ("be", "X-3") in r)
    check("8. X-1 terminée sans rien de nouveau : pas relancée en boucle", ("be", "X-1") not in r)
    check("9. aucun doublon", len(r) == len(set(r)))
    st = status()
    check("10. /status : temps réel connecté", st["workspaces"]["alpha"]["realtime_connected"] is True)
    check("10b. réglage mono-workspace : seul ce workspace est suivi", list(st["workspaces"]) == ["alpha"] and reruns("ws2") == [])

log1 = run(SINGLE, 3, scenario1)

# ---------- scenario 2: review-priority mode (3 reviews > limit 2)
fm.reset()
def scenario2():
    r = reruns()
    check("11. mode relecture prioritaire : aucune tâche « À faire » démarrée (X-2, X-3)", ("be", "X-2") not in r and ("be", "X-3") not in r)
    check("12. ... mais le travail « En cours » reprend (X-8, X-6) et les relectures avancent", ("be", "X-8") in r and ("be", "X-6") in r and ("qa", "X-11") in r)
log2 = run(dict(SINGLE, REVIEW_PRIORITY_LIMIT="2"), 3, scenario2)

# ---------- scenario 3: simulation mode never calls rerun
fm.reset()
def scenario3():
    check("13. DRY_RUN=1 : aucune relance réelle", reruns() == [])
log3 = run(dict(SINGLE, DRY_RUN="1"), 3, scenario3)

# ---------- scenario 4: every workspace (default), one excluded, review mode per workspace
fm.reset()
def scenario4():
    a, b = reruns("ws1"), reruns("ws2")
    check("14. découverte auto : alpha ET beta suivis, archive exclue",
          sorted(status()["workspaces"]) == ["alpha", "beta"] and reruns("ws3") == [])
    check("15. alpha en mode relecture prioritaire : ses « À faire » attendent", ("be", "X-2") not in a and ("qa", "X-11") in a)
    check("16. beta n'a pas de relectures en attente : ses « À faire » démarrent (mode isolé par workspace)",
          b[:1] == [("be2", "Y-2")] and ("fe2", "Y-4") in b)
    check("17. capacité respectée dans beta (2 places, 1 occupée -> 1 lancement backend)", [x for x in b if x[0] == "be2"] == [("be2", "Y-2")])
    st = status()["workspaces"]
    check("18. /status par workspace : alpha prioritaire, beta non",
          st["alpha"]["review_priority_mode"] is True and st["beta"]["review_priority_mode"] is False)
    check("19. une connexion temps réel par workspace", fm.connected_workspaces() == ["ws1", "ws2"])
    # An event in beta must wake beta's agent, not alpha's agent of the same name.
    with fm.LOCK:
        fm.STATE["ws"]["ws2"]["tasks"]["be2"][0].update(status="completed", completed_at=fm.iso())
    before_a = len(reruns("ws1"))
    fm.emit({"type": "task:completed", "payload": {"task_id": "u1", "agent_id": "be2", "issue_id": "j1"}}, "ws2")
    time.sleep(3)
    check("20. événement de beta -> place libérée dans beta -> Y-3 lancé", ("be2", "Y-3") in reruns("ws2"))
    check("21. ... et rien de neuf dans alpha", len(reruns("ws1")) == before_a)
    with fm.LOCK:
        check("22. aucune relance envoyée au mauvais workspace", not fm.STATE.get("cross_workspace"))
log4 = run({"EXCLUDE_WORKSPACES": "archive,gamma", "REVIEW_PRIORITY_LIMIT": "2"}, 3, scenario4)

# ---------- scenario 5: explicit list by slug + per-workspace agent filter
fm.reset()
def scenario5():
    check("23. MULTICA_WORKSPACES=beta : seul beta est suivi", list(status()["workspaces"]) == ["beta"] and reruns("ws1") == [])
    check("24. AGENTS=beta/frontend-lead : seul cet agent de beta est relancé", reruns("ws2") == [("fe2", "Y-4")])
log5 = run({"MULTICA_WORKSPACES": "beta", "AGENTS": "beta/frontend-lead"}, 3, scenario5)

# ---------- scenario 6: reviews left with the leads are handed to qa-lead
fm.reset()
def assignments(workspace="ws4"):
    with fm.LOCK:
        return [(i, a) for w, i, a in fm.STATE["assignments"] if w == workspace]
def scenario6():
    a = assignments()
    check("25. 2 places libres chez qa-lead -> les 2 relectures les plus anciennes lui sont confiées",
          a == [("G-2", "qa4"), ("G-4", "qa4")])
    check("26. jamais l'issue sur laquelle le lead travaille encore (G-3)", ("G-3", "qa4") not in a)
    check("27. jamais une relecture assignée à une personne (G-6) ni « Test après déploiement » (G-7)",
          ("G-6", "qa4") not in a and ("G-7", "qa4") not in a)
    check("28. qa-lead complet : G-5 attend", ("G-5", "qa4") not in a)
    # qa-lead finishes one review: a slot frees up, G-5 moves right away (event-driven)
    with fm.LOCK:
        fm.STATE["ws"]["ws4"]["tasks"]["qa4"][0].update(status="completed", completed_at=fm.iso())
        next(i for i in fm.STATE["ws"]["ws4"]["issues"] if i["id"] == "g1")["status"] = "done"
    fm.emit({"type": "task:completed", "payload": {"task_id": "q1", "agent_id": "qa4", "issue_id": "g1"}}, "ws4")
    time.sleep(4)
    check("29. fin d'une relecture de qa-lead -> la suivante (G-5) lui est confiée aussitôt", ("G-5", "qa4") in assignments())
    check("30. /status compte les relectures confiées", status()["workspaces"]["gamma"]["reviews_handed_over"] == 3)
    check("31. rien n'est réassigné ailleurs", assignments("ws1") == [] and assignments("ws2") == [])
log6 = run({"MULTICA_WORKSPACES": "gamma"}, 3, scenario6)

fm.reset()
def scenario7():
    check("32. REVIEW_ASSIGNEE vide : aucun responsable changé", assignments() == [])
log7 = run({"MULTICA_WORKSPACES": "gamma", "REVIEW_ASSIGNEE": ""}, 3, scenario7)

fm.reset()
def scenario8():
    check("33. DRY_RUN=1 : aucun transfert réel", assignments() == [])
log8 = run({"MULTICA_WORKSPACES": "gamma", "DRY_RUN": "1"}, 3, scenario8)

print("--- journal (scénario 1) ---"); print(log1)
print("--- journal (scénario 3, simulation) ---"); print(log3)
print("--- journal (scénario 4, multi-workspace) ---"); print(log4)
print("--- journal (scénario 6, transfert des relectures) ---"); print(log6)
print("--- journal (scénario 8, simulation du transfert) ---"); print(log8)
sys.exit(0 if ok else 1)
