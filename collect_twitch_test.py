"""Test Twitch Helix sur 10 minutes, avec 6 relevés (T+0, 2, 4, 6, 8, 10).

Lancement local : python collect_twitch_test.py
GitHub Actions : lancer manuellement le workflow collecte_twitch_test.yml.
Les résultats sont enregistrés dans twitch_helix_test_10min.csv (jamais dans le CSV de production).
"""

import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

DUREE_MINUTES = 10
INTERVALLE_SECONDES = 120
NOMBRE_RELEVES = DUREE_MINUTES * 60 // INTERVALLE_SECONDES + 1
MAX_STREAMS = 2000
MIN_VIEWERS = 1000
SORTIE = Path(__file__).resolve().parent / "twitch_helix_test_10min.csv"

COLONNES = [
    "scheduled_at", "collected_at", "stream_id", "user_id", "user_name",
    "game_id", "game_name", "viewer_count", "started_at", "language", "title",
    "source", "follower_count", "follower_status", "broadcaster_type",
    "stream_duration_minutes", "day_of_week", "hour_of_day", "viewer_follower_ratio",
]


def obtenir_jeton(session, client_id, client_secret):
    resultat = session.post(
        "https://id.twitch.tv/oauth2/token",
        data={"client_id": client_id, "client_secret": client_secret,
              "grant_type": "client_credentials"},
        timeout=30,
    )
    resultat.raise_for_status()
    return resultat.json()["access_token"]


def appeler(session, client_id, jeton, chemin, params=None):
    for essai in range(5):
        try:
            reponse = session.get(
                f"https://api.twitch.tv/helix{chemin}",
                headers={"Client-Id": client_id, "Authorization": f"Bearer {jeton}"},
                params=params,
                timeout=30,
            )
        except requests.RequestException:
            if essai == 4:
                raise
            time.sleep(min(10, 2 ** (essai + 1)))
            continue
        if reponse.status_code == 429:
            delai = float(reponse.headers.get("Ratelimit-Reset", time.time() + 30))
            time.sleep(max(1, min(90, delai - time.time() + 1)))
            continue
        if reponse.status_code >= 500:
            reponse.raise_for_status() if essai == 4 else time.sleep(2 ** (essai + 1))
            continue
        reponse.raise_for_status()
        return reponse.json()
    raise RuntimeError("Échec de la requête Helix après plusieurs essais")


def recuperer_streams(session, client_id, jeton):
    streams = {}
    cursor = None
    cursors_vus = set()
    while len(streams) < MAX_STREAMS:
        params = {"first": 100}
        if cursor:
            params["after"] = cursor
        resultat = appeler(session, client_id, jeton, "/streams", params)
        page = resultat.get("data", [])
        if not page:
            break
        sous_seuil = False
        for stream in page:
            viewers = int(stream.get("viewer_count") or 0)
            if viewers < MIN_VIEWERS:
                sous_seuil = True
                break
            streams.setdefault(stream["id"], stream)
            if len(streams) >= MAX_STREAMS:
                break
        if sous_seuil or len(streams) >= MAX_STREAMS:
            break
        prochain = resultat.get("pagination", {}).get("cursor")
        if not prochain or prochain in cursors_vus or prochain == cursor:
            break
        cursors_vus.add(prochain)
        cursor = prochain
    return list(streams.values())


def recuperer_statuts(session, client_id, jeton, streams):
    ids = list(dict.fromkeys(s["user_id"] for s in streams if s.get("user_id")))
    statuts = {}
    for depart in range(0, len(ids), 100):
        bloc = ids[depart:depart + 100]
        params = [("id", valeur) for valeur in bloc]
        resultat = appeler(session, client_id, jeton, "/users", params)
        for user in resultat.get("data", []):
            statuts[user["id"]] = user.get("broadcaster_type", "")
    return statuts


def recuperer_followers(session, client_id, jeton, streams):
    """Récupère le total public des followers pour chaque chaîne.

    La liste nominative nécessite des autorisations, pas le champ total.
    """
    ids = list(dict.fromkeys(s["user_id"] for s in streams if s.get("user_id")))
    followers = {}
    for numero, identifiant in enumerate(ids, 1):
        try:
            resultat = appeler(
                session, client_id, jeton, "/channels/followers",
                {"broadcaster_id": identifiant, "first": 1},
            )
            if "total" in resultat:
                followers[identifiant] = int(resultat["total"])
        except requests.RequestException as erreur:
            print(f"  Followers indisponibles pour {identifiant}: {erreur}", flush=True)
        if numero % 100 == 0:
            print(f"  Followers : {numero}/{len(ids)} chaînes vérifiées", flush=True)
    print(f"  Followers récupérés : {len(followers)}/{len(ids)}", flush=True)
    return followers


def enregistrer(streams, statuts, followers, date_prevue, date_collecte):
    fichier_vide = not SORTIE.exists() or SORTIE.stat().st_size == 0
    with SORTIE.open("a", encoding="utf-8-sig", newline="") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=COLONNES)
        if fichier_vide:
            writer.writeheader()
        for stream in streams:
            try:
                debut = datetime.fromisoformat(stream["started_at"].replace("Z", "+00:00"))
                duree = max(0, round((date_collecte - debut).total_seconds() / 60, 1))
            except (KeyError, ValueError, TypeError):
                duree = ""
            nombre_followers = followers.get(stream.get("user_id"))
            viewers = int(stream.get("viewer_count") or 0)
            writer.writerow({
                "scheduled_at": date_prevue.isoformat(),
                "collected_at": date_collecte.isoformat(),
                "stream_id": stream.get("id", ""),
                "user_id": stream.get("user_id", ""),
                "user_name": stream.get("user_name", ""),
                "game_id": stream.get("game_id", ""),
                "game_name": stream.get("game_name", ""),
                "viewer_count": stream.get("viewer_count", ""),
                "started_at": stream.get("started_at", ""),
                "language": stream.get("language", ""),
                "title": stream.get("title", ""),
                "source": "twitch_helix_test",
                "follower_count": nombre_followers if nombre_followers is not None else "",
                "follower_status": "available" if nombre_followers is not None else "unavailable",
                "broadcaster_type": statuts.get(stream.get("user_id"), ""),
                "stream_duration_minutes": duree,
                "day_of_week": date_collecte.strftime("%A"),
                "hour_of_day": date_collecte.hour,  # UTC
                "viewer_follower_ratio": round(viewers / nombre_followers, 6) if nombre_followers and nombre_followers > 0 else "",
            })


def main():
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    client_secret = os.environ.get("TWITCH_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Secrets TWITCH_CLIENT_ID et TWITCH_CLIENT_SECRET requis")

    # Nouveau test à chaque lancement : évite de mélanger des campagnes distinctes.
    if SORTIE.exists():
        SORTIE.unlink()

    with requests.Session() as session:
        jeton = obtenir_jeton(session, client_id, client_secret)
        depart = time.monotonic()
        for numero in range(NOMBRE_RELEVES):
            cible = depart + numero * INTERVALLE_SECONDES
            delai = cible - time.monotonic()
            if delai > 0:
                time.sleep(delai)
            date_prevue = datetime.now(timezone.utc)
            print(f"[{numero + 1}/{NOMBRE_RELEVES}] Relevé démarré à {date_prevue.isoformat()}", flush=True)
            try:
                streams = recuperer_streams(session, client_id, jeton)
                statuts = recuperer_statuts(session, client_id, jeton, streams)
                followers = recuperer_followers(session, client_id, jeton, streams)
                date_collecte = datetime.now(timezone.utc)
                enregistrer(streams, statuts, followers, date_prevue, date_collecte)
                print(f"  -> {len(streams)} streams enregistrés dans {SORTIE.name}", flush=True)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 401:
                    jeton = obtenir_jeton(session, client_id, client_secret)
                print(f"  -> Erreur API (relevé ignoré) : {exc}", flush=True)
            except (requests.RequestException, ValueError) as exc:
                print(f"  -> Erreur (relevé ignoré) : {exc}", flush=True)
    print(f"Test terminé. CSV : {SORTIE}", flush=True)


if __name__ == "__main__":
    main()
