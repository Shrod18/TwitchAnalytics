"""Une collecte Twitch Helix par exécution (GitHub Actions, toutes les 4 h).
La campagne s’arrête après 42 relevés ou après 7 jours UTC depuis le premier.
Les relevés sont conservés dans un CSV unique, versionné par le workflow.
Variables d’environnement : TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET.
"""
import csv
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ---------------- PARAMÈTRES ----------------
MAX_STREAMS_PAR_RELEVE = 2000
MIN_SPECTATEURS = 1000
INTERVALLE_HEURES = 4
DUREE_JOURS = 7
FICHIER_CSV = Path(__file__).resolve().parent / "twitch_helix_7jours.csv"

CHAMPS = [
    "scheduled_at", "collected_at", "stream_id", "user_id", "user_name",
    "game_id", "game_name", "viewer_count", "started_at", "language",
    "title", "source",
]


def utc_now():
    return datetime.now(timezone.utc)


def obtenir_token(session, client_id, secret):
    r = session.post(
        "https://id.twitch.tv/oauth2/token",
        data={"client_id": client_id, "client_secret": secret,
              "grant_type": "client_credentials"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def charger_slots_existants():
    """Reprendre depuis le CSV, sans créer de second fichier d'état."""
    if not FICHIER_CSV.exists() or FICHIER_CSV.stat().st_size == 0:
        return None, set()
    with FICHIER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        lecteur = csv.DictReader(f)
        if not lecteur.fieldnames or "scheduled_at" not in lecteur.fieldnames:
            raise ValueError("Le CSV existe mais n'a pas le format attendu. Sauvegardez-le avant de continuer.")
        slots = {row["scheduled_at"] for row in lecteur if row.get("scheduled_at")}
    if not slots:
        raise ValueError("CSV existant sans relevé complet : vérifier avant de relancer.")
    # Le premier slot est le début fixé de la campagne.
    debut = min(datetime.fromisoformat(s) for s in slots)
    return debut, slots


def appel_streams(session, client_id, secret, token, params):
    """Retourne (json, token). Retry API 429/5xx et réauthentification 401."""
    for tentative in range(6):
        try:
            r = session.get(
                "https://api.twitch.tv/helix/streams",
                headers={"Client-Id": client_id, "Authorization": f"Bearer {token}"},
                params=params,
                timeout=35,
            )
        except requests.RequestException as exc:
            if tentative == 5:
                raise
            attente = min(60, 2 ** (tentative + 1))
            print(f"Erreur réseau ({exc}) ; nouvel essai dans {attente}s")
            time.sleep(attente)
            continue
        if r.status_code == 401:
            token = obtenir_token(session, client_id, secret)
            continue
        if r.status_code == 429:
            reset = float(r.headers.get("Ratelimit-Reset", time.time() + 60))
            attente = max(1, min(600, reset - time.time() + 1))
            print(f"Quota API : attente {attente:.0f}s")
            time.sleep(attente)
            continue
        if 500 <= r.status_code < 600:
            if tentative == 5:
                r.raise_for_status()
            attente = min(60, 2 ** (tentative + 1))
            print(f"Erreur serveur {r.status_code} ; nouvel essai dans {attente}s")
            time.sleep(attente)
            continue
        r.raise_for_status()
        return r.json(), token
    raise RuntimeError("Échec des requêtes après plusieurs tentatives")


def collecter(session, client_id, secret, token):
    streams = {}
    curseur = None
    deja_vus = set()
    # Seuil appliqué localement : Helix ne propose pas de filtre viewer_count minimal.
    # Les résultats sont triés par audience décroissante.
    while len(streams) < MAX_STREAMS_PAR_RELEVE:
        params = {"first": 100}
        if curseur:
            params["after"] = curseur
        resultat, token = appel_streams(session, client_id, secret, token, params)
        page = resultat.get("data", [])
        if not page:
            break
        seuil_franchi = False
        for s in page:
            audience = int(s.get("viewer_count") or 0)
            if audience < MIN_SPECTATEURS:
                seuil_franchi = True
                break
            streams.setdefault(s["id"], s)
            if len(streams) >= MAX_STREAMS_PAR_RELEVE:
                break
        if seuil_franchi or len(streams) >= MAX_STREAMS_PAR_RELEVE:
            break
        nouveau_curseur = resultat.get("pagination", {}).get("cursor")
        if not nouveau_curseur or nouveau_curseur == curseur or nouveau_curseur in deja_vus:
            break
        deja_vus.add(nouveau_curseur)
        curseur = nouveau_curseur
    return list(streams.values()), token


def enregistrer(streams, slot, date_releve):
    """Append d'un relevé au CSV unique."""
    nouveau = not FICHIER_CSV.exists() or FICHIER_CSV.stat().st_size == 0
    with FICHIER_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CHAMPS)
        if nouveau:
            w.writeheader()
        for s in streams:
            w.writerow({
                "scheduled_at": slot.isoformat(),
                "collected_at": date_releve.isoformat(),
                "stream_id": s["id"],
                "user_id": s.get("user_id", ""),
                "user_name": s.get("user_name", ""),
                "game_id": s.get("game_id", ""),
                "game_name": s.get("game_name", ""),
                "viewer_count": int(s["viewer_count"]),
                "started_at": s.get("started_at", ""),
                "language": s.get("language", ""),
                "title": s.get("title", ""),
                "source": "twitch_helix",
            })
        f.flush()
        os.fsync(f.fileno())


def main():
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    secret = os.environ.get("TWITCH_CLIENT_SECRET")
    if not client_id or not secret:
        raise RuntimeError("Secrets TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET absents.")

    debut, slots = charger_slots_existants()
    maintenant = utc_now()
    nb_max = DUREE_JOURS * 24 // INTERVALLE_HEURES  # 42

    if debut is not None:
        if len(slots) >= nb_max or maintenant >= debut + timedelta(days=DUREE_JOURS):
            print("Campagne terminée : aucun nouveau relevé.")
            return
        # Protège contre plusieurs lancements dans la même fenêtre de 4 heures.
        if slots:
            dernier = max(datetime.fromisoformat(slot) for slot in slots)
            if maintenant < dernier + timedelta(hours=INTERVALLE_HEURES - 0.25):
                print("Relevé déjà effectué récemment : aucune modification.")
                return

    # La première exécution fixe le début réel de la campagne.
    slot = maintenant.replace(microsecond=0)
    print(f"Collecte UTC : {slot.isoformat()}")
    session = requests.Session()
    try:
        token = obtenir_token(session, client_id, secret)
        streams, _ = collecter(session, client_id, secret, token)
        if not streams:
            print("Aucun stream éligible : pas de ligne artificielle.")
            return
        enregistrer(streams, slot, utc_now())
        print(f"Relevé enregistré : {len(streams)} streams >= {MIN_SPECTATEURS} spectateurs.")
        print(f"CSV cumulatif : {FICHIER_CSV}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
