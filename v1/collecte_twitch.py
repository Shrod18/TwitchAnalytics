
import os
import csv
import time
import requests
from pathlib import Path
from datetime import datetime, timezone

CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]

NB_STREAMS = 10000
FICHIER = Path(__file__).resolve().parent / "echantillon_twitch_api.csv"

# Authentification Twitch
auth = requests.post(
    "https://id.twitch.tv/oauth2/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    },
    timeout=20
)
auth.raise_for_status()
token = auth.json()["access_token"]

headers = {
    "Client-Id": CLIENT_ID,
    "Authorization": f"Bearer {token}"
}

streams = {}
cursor = None

print("Récupération des données Twitch...")

while len(streams) < NB_STREAMS:
    params = {
        "first": min(100, NB_STREAMS - len(streams))
    }

    if cursor:
        params["after"] = cursor

    # Gestion des limites de l'API
    for tentative in range(5):
        response = requests.get(
            "https://api.twitch.tv/helix/streams",
            headers=headers,
            params=params,
            timeout=30
        )

        if response.status_code == 429:
            reset = int(response.headers.get(
                "Ratelimit-Reset", time.time() + 60
            ))
            time.sleep(max(1, reset - time.time() + 1))
            continue

        response.raise_for_status()
        break
    else:
        raise RuntimeError("Limite API atteinte après 5 essais")

    resultat = response.json()
    donnees = resultat.get("data", [])

    if not donnees:
        break

    for stream in donnees:
        streams[stream["id"]] = stream

    print(f"{len(streams)} streams récupérés")

    cursor = resultat.get("pagination", {}).get("cursor")

    if not cursor:
        break

# Export immédiat dans un seul CSV
colonnes = [
    "id", "user_id", "user_name",
    "game_id", "game_name", "title",
    "viewer_count", "started_at", "language",
    "collected_at"
]

date_collecte = datetime.now(timezone.utc).isoformat()

with open(FICHIER, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=colonnes)
    writer.writeheader()

    for stream in streams.values():
        writer.writerow({
            col: (
                date_collecte if col == "collected_at"
                else stream.get(col)
            )
            for col in colonnes
        })

print("\nEXPORT TERMINÉ")
print(f"Streams : {len(streams)}")
print(f"Fichier : {FICHIER}")
