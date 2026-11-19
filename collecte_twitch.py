"""Collecte Twitch Helix enrichie, pour GitHub Actions toutes les 4 heures.

Une exécution = un relevé. Un seul CSV cumulatif, compatible avec l'ancien CSV.
Secrets GitHub : TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET.
Le champ total des followers est accessible avec un token d'application.
La liste des followers, en revanche, nécessite des autorisations utilisateur.
"""
import csv
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

MAX_STREAMS_PAR_RELEVE = 5000
MIN_SPECTATEURS = 1000
INTERVALLE_HEURES = 4
DUREE_JOURS = 7
FICHIER_CSV = Path(__file__).resolve().parent / 'twitch_helix_7jours.csv'

ANCIENS_CHAMPS = [
    'scheduled_at', 'collected_at', 'stream_id', 'user_id', 'user_name',
    'game_id', 'game_name', 'viewer_count', 'started_at', 'language',
    'title', 'source',
]
NOUVEAUX_CHAMPS = [
    'follower_count', 'follower_status', 'broadcaster_type',
    'stream_duration_minutes', 'day_of_week', 'hour_of_day',
    'viewer_follower_ratio',
]
CHAMPS = ANCIENS_CHAMPS + NOUVEAUX_CHAMPS
BASE_URL = 'https://api.twitch.tv/helix'


def maintenant_utc():
    return datetime.now(timezone.utc)


def date_utc(texte):
    return datetime.fromisoformat(str(texte).replace('Z', '+00:00')).astimezone(timezone.utc)


def obtenir_token(session, client_id, secret):
    reponse = session.post('https://id.twitch.tv/oauth2/token', data={
        'client_id': client_id,
        'client_secret': secret,
        'grant_type': 'client_credentials',
    }, timeout=30)
    reponse.raise_for_status()
    return reponse.json()['access_token']


class API:
    def __init__(self, session, client_id, secret):
        self.session = session
        self.client_id = client_id
        self.secret = secret
        self.token = obtenir_token(session, client_id, secret)

    def get(self, endpoint, params=None, user_token=None, optional=False):
        """Retry quota/5xx/réseau ; erreurs 401/403 facultatives -> None."""
        for tentative in range(6):
            token = user_token if user_token else self.token
            try:
                reponse = self.session.get(
                    BASE_URL + endpoint,
                    headers={'Client-Id': self.client_id,
                             'Authorization': f'Bearer {token}'},
                    params=params, timeout=35,
                )
            except requests.RequestException:
                if tentative == 5:
                    if optional:
                        return None
                    raise
                time.sleep(min(30, 2 ** (tentative + 1)))
                continue
            if reponse.status_code == 401 and not user_token:
                self.token = obtenir_token(self.session, self.client_id, self.secret)
                if tentative < 5:
                    continue
            if reponse.status_code in (401, 403) and optional:
                return None
            if reponse.status_code == 429:
                reset = float(reponse.headers.get('Ratelimit-Reset', time.time() + 60))
                time.sleep(max(1, min(600, reset - time.time() + 1)))
                continue
            if reponse.status_code >= 500:
                if tentative == 5:
                    if optional:
                        return None
                    reponse.raise_for_status()
                time.sleep(min(30, 2 ** (tentative + 1)))
                continue
            if optional and not reponse.ok:
                return None
            reponse.raise_for_status()
            return reponse.json()
        if optional:
            return None
        raise RuntimeError('Échec API après plusieurs tentatives')


def lire_csv_existant():
    if not FICHIER_CSV.exists() or FICHIER_CSV.stat().st_size == 0:
        return [], None, set()
    with FICHIER_CSV.open('r', encoding='utf-8-sig', newline='') as fichier:
        lecteur = csv.DictReader(fichier)
        colonnes = lecteur.fieldnames or []
        if not set(ANCIENS_CHAMPS).issubset(colonnes):
            raise ValueError('CSV existant incompatible : sauvegardez-le avant modification.')
        lignes = list(lecteur)
    slots = {ligne['scheduled_at'] for ligne in lignes if ligne.get('scheduled_at')}
    if not slots:
        raise ValueError('CSV existant sans créneau horodaté.')
    return lignes, min(date_utc(s) for s in slots), slots


def collecter_streams(api):
    streams = {}
    curseur = None
    curseurs_vus = set()
    while len(streams) < MAX_STREAMS_PAR_RELEVE:
        params = {'first': 100}
        if curseur:
            params['after'] = curseur
        resultat = api.get('/streams', params)
        page = resultat.get('data', [])
        if not page:
            break
        seuil_atteint = False
        for stream in page:
            audience = int(stream.get('viewer_count') or 0)
            if audience < MIN_SPECTATEURS:
                seuil_atteint = True
                break
            streams.setdefault(stream['id'], stream)
            if len(streams) >= MAX_STREAMS_PAR_RELEVE:
                break
        if seuil_atteint or len(streams) >= MAX_STREAMS_PAR_RELEVE:
            break
        prochain = resultat.get('pagination', {}).get('cursor')
        if not prochain or prochain == curseur or prochain in curseurs_vus:
            break
        curseurs_vus.add(prochain)
        curseur = prochain
    return list(streams.values())


def types_de_comptes(api, ids):
    """Get Users accepte jusqu'à 100 identifiants par requête."""
    resultat = {}
    for debut in range(0, len(ids), 100):
        bloc = ids[debut:debut + 100]
        donnees = api.get('/users', [('id', identifiant) for identifiant in bloc])
        for user in donnees.get('data', []):
            resultat[user['id']] = user.get('broadcaster_type', '')
    return resultat


def followers_publics(api, ids):
    """Total public des followers, accessible avec le jeton d'application."""
    resultats = {}
    for numero, identifiant in enumerate(ids, 1):
        resultat = api.get('/channels/followers',
                           {'broadcaster_id': identifiant, 'first': 1},
                           optional=True)
        if resultat is not None and 'total' in resultat:
            resultats[identifiant] = int(resultat['total'])
        if numero % 100 == 0:
            print(f'Followers : {numero}/{len(ids)} chaînes vérifiées.')
    print(f'Followers disponibles : {len(resultats)}/{len(ids)}.')
    return resultats


def lignes_nouvelles(streams, slot, releve, api):
    ids = list(dict.fromkeys(s['user_id'] for s in streams if s.get('user_id')))
    types = types_de_comptes(api, ids)
    followers = followers_publics(api, ids)
    lignes = []
    for s in streams:
        debut_live = date_utc(s['started_at']) if s.get('started_at') else None
        duree = max(0, round((releve - debut_live).total_seconds() / 60, 1)) if debut_live else ''
        count = followers.get(s.get('user_id'))
        audience = int(s.get('viewer_count') or 0)
        lignes.append({
            'scheduled_at': slot.isoformat(),
            'collected_at': releve.isoformat(),
            'stream_id': s['id'],
            'user_id': s.get('user_id', ''),
            'user_name': s.get('user_name', ''),
            'game_id': s.get('game_id', ''),
            'game_name': s.get('game_name', ''),
            'viewer_count': audience,
            'started_at': s.get('started_at', ''),
            'language': s.get('language', ''),
            'title': s.get('title', ''),
            'source': 'twitch_helix',
            'follower_count': count if count is not None else '',
            'follower_status': 'available' if count is not None else 'unavailable',
            'broadcaster_type': types.get(s.get('user_id'), ''),
            'stream_duration_minutes': duree,
            'day_of_week': releve.strftime('%A'),
            'hour_of_day': releve.hour,  # UTC
            'viewer_follower_ratio': round(audience / count, 6) if count and count > 0 else '',
        })
    return lignes


def enregistrer_atomiquement(anciennes, nouvelles):
    """Migration des anciens CSV + nouvelles lignes en une seule écriture atomique."""
    fd, nom_temporaire = tempfile.mkstemp(prefix='.twitch_', suffix='.csv',
                                          dir=str(FICHIER_CSV.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8-sig', newline='') as fichier:
            writer = csv.DictWriter(fichier, fieldnames=CHAMPS, extrasaction='ignore')
            writer.writeheader()
            for ligne in anciennes + nouvelles:
                writer.writerow(ligne)
            fichier.flush()
            os.fsync(fichier.fileno())
        os.replace(nom_temporaire, FICHIER_CSV)
    finally:
        if os.path.exists(nom_temporaire):
            os.remove(nom_temporaire)


def main():
    client_id = os.environ.get('TWITCH_CLIENT_ID')
    secret = os.environ.get('TWITCH_CLIENT_SECRET')
    if not client_id or not secret:
        raise RuntimeError('Secrets TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET absents.')

    anciennes, debut, slots = lire_csv_existant()
    maintenant = maintenant_utc()
    max_collectes = DUREE_JOURS * 24 // INTERVALLE_HEURES
    if debut is not None:
        if len(slots) >= max_collectes or maintenant >= debut + timedelta(days=DUREE_JOURS):
            print('Campagne déjà terminée : aucun nouveau relevé.')
            return
        dernier = max(date_utc(slot) for slot in slots)
        if maintenant < dernier + timedelta(hours=INTERVALLE_HEURES - 0.25):
            print('Collecte récente déjà enregistrée : rien à faire.')
            return

    slot = maintenant.replace(microsecond=0)
    with requests.Session() as session:
        api = API(session, client_id, secret)
        streams = collecter_streams(api)
        if not streams:
            print('Aucun stream ayant au moins 1000 spectateurs.')
            return
        releve = maintenant_utc()
        nouvelles = lignes_nouvelles(streams, slot, releve, api)
        enregistrer_atomiquement(anciennes, nouvelles)
        print(f'{len(nouvelles)} observations ajoutées au CSV {FICHIER_CSV.name}')
        print(f'Statut de chaîne disponible pour {sum(bool(x["broadcaster_type"]) for x in nouvelles)} streams.')
        print(f'Followers disponibles pour {sum(x["follower_status"] == "available" for x in nouvelles)} streams.')
        print(f'Total CSV : {len(anciennes) + len(nouvelles)} lignes.')


if __name__ == '__main__':
    main()
