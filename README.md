# multica-dispatcher — tes agents Multica travaillent sans s'arrêter

Multica ne lance une exécution que sur un événement : assignation, mention, réveil, autopilot. Quand un agent termine, rien ne lui fait prendre sa tâche suivante. Les cartes « À faire » et « En cours » dorment alors jusqu'à ton prochain passage.

Ce service corrige ça. Dès qu'un agent libère une place, il lance aussitôt sa prochaine tâche éligible. Il n'utilise pas d'IA : il ne consomme aucun token.

**Multi-workspace.** Une seule instance suit tous tes projets. Par défaut (`MULTICA_WORKSPACES=all`), elle prend tous les workspaces accessibles avec ton token et détecte ceux que tu crées ensuite, au prochain rattrapage. Chaque workspace est traité à part : sa propre connexion temps réel, ses agents, son compteur de relectures et son mode relecture prioritaire. Un projet saturé de relectures ne freine pas les autres.

| Réglage | Effet |
| --- | --- |
| `MULTICA_WORKSPACES=all` (défaut) | Tous les workspaces du token, nouveaux compris |
| `MULTICA_WORKSPACES=alpha,beta` | Seulement ces workspaces (slugs ou identifiants) |
| `EXCLUDE_WORKSPACES=archive` | À ignorer, par exemple un projet archivé |
| `AGENTS=qa-lead` | Cet agent dans tous les workspaces (vide = tous les agents) |
| `AGENTS=beta/qa-lead` | Cet agent dans le workspace `beta` seulement |

L'ancien réglage `MULTICA_WORKSPACE_ID` (un seul workspace) marche toujours quand `MULTICA_WORKSPACES` est vide.

Le slug d'un workspace s'affiche avec `multica workspace list`.

## Comment il fonctionne

1. **En temps réel.** Il écoute le WebSocket de Multica : fin d'exécution (réussie, échouée ou annulée) et changement de statut ou de responsable d'une issue. Après l'événement, il attend `DEBOUNCE_SECONDS` (3 secondes) pour regrouper une rafale d'événements, puis relance les agents concernés.
2. **Rattrapage.** Toutes les `RECONCILE_SECONDS` (60 secondes), il relit la liste des workspaces, puis repasse sur tous les agents de chacun, au cas où un événement aurait été perdu (coupure réseau, redémarrage). C'est un filet de sécurité, pas le moteur principal.
3. **Lancement.** Il appelle `POST /api/issues/{id}/rerun`, le même appel que le bouton « Relancer » de Multica. L'agent assigné reprend alors sa session sur l'issue.

## Quelles issues il lance

Pour chaque agent, il calcule ses places libres : `max_concurrent_tasks` moins ses exécutions en cours et en file. Il lance ensuite, au plus ce nombre, les issues qui remplissent toutes ces conditions :

| Condition | Détail |
| --- | --- |
| Assignée à l'agent | Les issues non assignées restent à Mika |
| Statut « À faire » ou « En cours » | Ou un statut personnalisé de catégorie `todo` / `in_progress` |
| « En relecture » | Seulement pour `REVIEWER_AGENTS` (qa-lead, security-lead, seo-lead) |
| Hors colonnes exclues | `EXCLUDE_STATUS_REGEX` : « Question », « Test après déploiement » attendent un humain |
| Jamais « À planifier », « Bloqué », « Terminé », « Annulé » | |
| Aucune exécution déjà en cours ou en file sur l'issue | Vérifié deux fois, dont juste avant le lancement |
| Dernière exécution de l'agent sur l'issue | Voir le tableau suivant |

| Dernière exécution | Décision |
| --- | --- |
| Aucune | Lancée |
| Échouée (connexion expirée, machine en veille, limite d'usage…) | Relancée |
| Annulée par le système | Relancée |
| Annulée par un humain | Jamais relancée : ta décision est respectée |
| Terminée normalement | Relancée seulement s'il s'est passé quelque chose depuis (commentaire, changement…). Sinon, l'agent attend un événement : on ne le relance pas en boucle |

Les issues sont prises par priorité (urgente, haute, moyenne, basse, aucune), puis de la plus ancienne à la plus récente.

**Mode relecture prioritaire.** Au-delà de `REVIEW_PRIORITY_LIMIT` issues en relecture dans un workspace (10 par défaut), les agents de ce workspace qui ne relisent pas ne démarrent plus de nouvelle tâche « À faire ». Ils reprennent seulement leur travail « En cours », dont les corrections demandées. Les relecteurs, eux, continuent.

**Transfert des relectures.** Le service relance l'agent assigné, mais une relecture restée chez le lead qui a fait le travail n'est relancée pour personne. Le service la confie donc à `REVIEW_ASSIGNEE` (`qa-lead` par défaut) :

- seulement dans la limite des places libres de qa-lead (`max_concurrent_tasks` moins ses exécutions actives et en file) ; les suivantes partent dès qu'une de ses relectures se termine, ou au rattrapage ;
- les plus prioritaires puis les plus anciennes d'abord ;
- jamais une issue sur laquelle le lead a encore une exécution en cours ou en file ;
- jamais une issue assignée à une personne, non assignée, déjà chez un relecteur (`REVIEWER_AGENTS`), ni dans une colonne exclue (« Test après déploiement », « Question ») ;
- l'assignation suffit : Multica lance alors lui-même l'exécution de qa-lead.

Avec `REVIEW_ASSIGNEE=` (vide), le service ne change jamais le responsable d'une issue. `/status` indique le nombre de relectures confiées depuis le démarrage (`reviews_handed_over`).

## Garde-fous

| Garde-fou | Défaut | Effet |
| --- | --- | --- |
| `DRY_RUN` | `1` | Simulation : journalise ce qu'il ferait, ne lance rien |
| `MAX_DISPATCH_PER_AGENT_PER_HOUR` | 20 | Plafond de lancements par agent et par heure |
| `ISSUE_COOLDOWN_SECONDS` | 300 | Délai minimal entre deux relances d'une même issue |
| `NO_PROGRESS_LIMIT` | 3 | Après 3 relances sans changement de statut, de responsable ou de contenu, l'issue est mise de côté. Elle redevient éligible dès qu'elle change |
| `MAX_DISPATCH_PER_ISSUE_PER_DAY` | 8 | Plafond de relances par issue et par jour |
| `FAILURE_BREAKER_*` | 3 échecs en 10 min → pause de 15 min | Si un agent échoue en série (connexion Claude expirée…), ses relances sont suspendues au lieu de brûler des exécutions |
| `ACTIVITY_GRACE_SECONDS` | 120 | Une activité moins de 2 minutes après la fin d'une exécution (son propre commentaire de fin) ne compte pas comme du nouveau |

L'historique par issue (relances, mises de côté) est conservé dans `/data/state.json`, sur un volume persistant.

## Installation sur Coolify (recommandé : serveur toujours allumé)

1. **Pousse ce dossier dans un dépôt Git** (privé), par exemple `multica-dispatcher`.
2. Dans Coolify : **New Resource → Docker Compose**, puis choisis le dépôt. Coolify lit `docker-compose.yml`.
3. **Variables d'environnement** (Coolify → Environment Variables), à partir de `.env.example` :
   - `MULTICA_SERVER_URL`, `MULTICA_TOKEN` : les valeurs `server_url` et `token` de ton `~/.multica/config.json` ;
   - `MULTICA_WORKSPACES` : laisse `all` pour suivre tous tes projets, ou liste les slugs voulus ;
   - laisse `DRY_RUN=1` pour commencer.
4. **Déploie**, puis lis les journaux. Tu dois y voir, pour chaque projet, `[<slug>] workspace suivi` puis `[<slug>] temps réel connecté`, et enfin des lignes `[simulation] lancerait [<slug>] …`.
5. Si les décisions te conviennent pendant quelques heures, passe à `DRY_RUN=0` et redéploie.

Le service n'ouvre aucun port public. L'état se consulte depuis le serveur :

```bash
docker exec -it <conteneur> python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8080/status').read().decode())"
```

Cette commande affiche, pour chaque workspace :
- l'état du temps réel ;
- le nombre de relectures et le mode prioritaire ;
- les lancements de la dernière heure, par agent ;
- les agents en pause ;
- les issues mises de côté.

## Vérifier en simulation (`DRY_RUN=1`)

En simulation, le service décide comme en réel mais ne lance rien. Il n'écrit rien non plus dans l'historique : pas de mise de côté, pas de plafond journalier, `state.json` intact. Le passage à `DRY_RUN=0` part donc d'un historique propre. Seul le délai entre deux relances (`ISSUE_COOLDOWN_SECONDS`, 5 minutes) est appliqué : une issue qui reste éligible réapparaît dans les journaux toutes les 5 minutes, c'est normal puisque rien n'a été lancé.

`/status` liste les 50 dernières relances simulées par workspace (`simulated_launches`).

| Vérification | Comment | Attendu |
| --- | --- | --- |
| Chaque ligne « lancerait » est juste | Ouvre l'issue dans Multica | Assignée à cet agent, « À faire » ou « En cours » (ou « En relecture » pour un relecteur), aucune exécution en cours, pas dans Question ni Test après déploiement |
| Rien de bloquant n'est relancé | Cherche dans les journaux une issue en Question, À planifier, Test après déploiement, ou annulée par toi | Elle n'apparaît jamais |
| Capacité respectée | Compte les lignes d'un agent sur un même passage | Pas plus que ses places libres (`max_concurrent_tasks` moins ses exécutions actives et en file) |
| Réaction aux événements | Passe une issue d'un agent de « À faire » à « En cours » | Ligne « lancerait … (issue modifiée ; …) » en moins de 5 secondes, sauf si l'issue est déjà dans son délai de 5 minutes |
| Fin d'exécution | Attends qu'un agent termine | Ligne « (task:completed ; …) » pour sa tâche suivante |
| Une exécution démarre | Relance une issue à la main dans Multica | Elle disparaît des lignes « lancerait » |
| Mode relecture prioritaire | `/status` | `review_priority_mode: true` dès que plus de `REVIEW_PRIORITY_LIMIT` relectures ; aucune « À faire » des agents non relecteurs |
| Reconnexion | Redéploie, ou attends une coupure | `temps réel déconnecté …` suivi de `temps réel connecté` en quelques secondes |

## Lancer sans Docker (test local)

```bash
pip install -r requirements.txt
DRY_RUN=1 STATE_FILE=./state.json python3 dispatcher.py
```

Sans variables `MULTICA_*`, le script lit `~/.multica/config.json`.

## Tests

```bash
./run-tests.sh
```

Les tests tournent sur un faux serveur Multica local, avec l'API et le WebSocket, et ne font aucun appel réel :

- **34 vérifications de bout en bout** : priorités, colonnes exclues, exécution échouée, exécution annulée par un humain, agent qui attend, relecture, lancement immédiat sur événement, mode relecture prioritaire, simulation ; et en multi-workspace : découverte automatique, exclusion, mode relecture isolé par workspace, événement routé vers le bon workspace, aucune relance envoyée au mauvais workspace, liste explicite par slug, filtre d'agent par workspace, ancien réglage mono-workspace ; transfert des relectures à qa-lead dans sa capacité, relais immédiat à la fin d'une relecture, issues protégées (lead encore au travail, personne, colonnes exclues), transfert désactivable, simulation ;
- **13 tests unitaires** : garde-fous, sélection des workspaces, simulation sans effet sur l'historique.

## Où il se branche dans Multica

Tout passe par l'API publique de Multica, vérifiée dans son code source en version 0.7 :

- **Workspaces** : `GET /api/workspaces`, la liste de ceux dont le token est membre. Chaque appel suivant porte l'en-tête `X-Workspace-ID` du workspace concerné.
- **Écoute** : une connexion WebSocket `/ws?workspace_id=…` par workspace, avec un premier message d'authentification `{"type":"auth","payload":{"token":…}}`, et les événements `task:completed`, `task:failed`, `task:cancelled` et `issue:updated`.
- **Lecture** : `GET /api/agents`, `GET /api/agents/{id}/tasks` et `GET /api/issues?assignee_id=…&open_only=true`.
- **Transfert d'une relecture** : `PUT /api/issues/{id}` avec `{"assignee_type":"agent","assignee_id":…}`, le même appel que `multica issue assign`.
- **Lancement** : `POST /api/issues/{id}/rerun`. Attention, cet appel annule une exécution en cours sur l'issue. C'est pour ça que le service vérifie juste avant qu'il n'y en a aucune.

Les lancements apparaissent dans Multica comme faits par toi, puisque le service utilise ton token.

## Limites connues

- **Le service lance le travail, il ne l'exécute pas.** Le daemon Multica et Claude Code doivent tourner sur une machine allumée, avec une connexion Claude valide.
- **Les issues non assignées** ne sont pas traitées : c'est le rôle de Mika.
- **Le token doit être membre de chaque workspace à suivre.** Un workspace auquel il n'a pas accès est signalé dans les journaux, puis ignoré.
- **Un nouveau workspace** est pris en compte au rattrapage suivant, soit 60 secondes au plus.
- **Si Multica change son API ou ses événements**, il faudra adapter le service. Les noms utilisés sont listés plus haut.

C'est la version « externe » (option B). Si elle fait ses preuves, la même logique pourra être proposée directement dans Multica (option A).
