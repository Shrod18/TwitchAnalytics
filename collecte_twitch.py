"""Twitch Analytics — Import des snapshots publics mingt2019/Twitch-Dataset (2017)
+ collecte Twitch Helix actuelle, export unique CSV avec provenance.

Installation : pip install requests pandas
Exemples :
  python collecte_twitch.py --historique-seul
  python collecte_twitch.py --limit 1000
  python collecte_twitch.py --limit 10000 --tous-fichiers

La période 2017 et la période actuelle ne sont PAS contiguës.
"""
import argparse
import ast
import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

REPO = 'mingt2019/Twitch-Dataset'
ROOT = Path(__file__).resolve().parent
COLUMNS = ['id','user_id','user_name','game_id','game_name','title',
           'viewer_count','started_at','language','collected_at','source']
ALIASES = {
    'id':['id','stream_id','streamid'],
    'user_id':['user_id','streamer_id','broadcaster_id','channel_id'],
    'user_name':['user_name','streamer_name','broadcaster_name','channel_name','streamer'],
    'game_id':['game_id','gameid'],
    'game_name':['game_name','game_title','game'],
    'title':['title','stream_title'],
    'viewer_count':['viewer_count','viewers','current_views','current_viewers','viewercount'],
    'started_at':['started_at','stream_created_at','created_at'],
    'language':['language','lang'],
}

def get_retry(session,url, **kw):
    for i in range(5):
        r=session.get(url,timeout=35,**kw)
        if r.status_code==429:
            time.sleep(max(1, float(r.headers.get('Ratelimit-Reset',time.time()+5))-time.time()+1));continue
        if r.status_code>=500:
            time.sleep(min(20,2**i));continue
        r.raise_for_status();return r
    raise RuntimeError('Requêtes impossibles après 5 essais : '+url)

def normalize_records(obj):
    """Trouve des objets structurés sans déduire des colonnes non documentées."""
    if isinstance(obj,list):
        if obj and all(isinstance(x,dict) for x in obj):return obj
        out=[]
        for x in obj:out.extend(normalize_records(x))
        return out
    if isinstance(obj,dict):
        keys={str(k).lower() for k in obj}
        if keys.intersection({'viewer_count','viewers','current_views','current_viewers'}):return [obj]
        out=[]
        for val in obj.values():out.extend(normalize_records(val))
        return out
    return []

def parse_text(data,filename):
    text=data.decode('utf-8-sig',errors='replace').strip()
    if not text:return []
    for parser in (json.loads,ast.literal_eval):
        try:
            rows=normalize_records(parser(text))
            if rows:return rows
        except (ValueError,SyntaxError,TypeError,RecursionError):pass
    rows=[]
    for line in text.splitlines():
        try:rows.extend(normalize_records(json.loads(line)))
        except (ValueError,TypeError):
            try: rows.extend(normalize_records(ast.literal_eval(line)))
            except (ValueError,SyntaxError,TypeError):pass
    if rows:return rows
    # Format original mingt2019 : 9 champs TSV sans en-tête,
    # lignes vides entre chaque stream. Ordre documenté par le README du dépôt.
    colonnes_tsv = [
        'streamer_id', 'language', 'game_id', 'started_at',
        'stream_type', 'stream_id', 'viewer_count',
        'historical_viewer_count', 'streamer_type'
    ]
    lignes = [line.strip() for line in text.splitlines() if line.strip()]
    if lignes:
        enregistrements = [line.split('\t') for line in lignes]
        if all(len(valeurs) == 9 for valeurs in enregistrements):
            donnees = [dict(zip(colonnes_tsv, valeurs)) for valeurs in enregistrements]
            # Évite de prendre un TSV inconnu à 9 champs pour le format Twitch.
            if all(x['viewer_count'].isdigit() and x['stream_id'].isdigit()
                   and x['streamer_id'].isdigit() and x['started_at'].endswith('Z')
                   for x in donnees):
                return donnees
    try:
        delimiter=csv.Sniffer().sniff(text[:4096],delimiters=',;\t|').delimiter
        rows=list(csv.DictReader(io.StringIO(text),delimiter=delimiter))
        if rows and any(x.lower().strip() in sum(ALIASES.values(),[]) for x in rows[0]):return rows
    except csv.Error:pass
    raise ValueError(f'Format de {filename} non reconnu : aperçu {text[:180]!r}. '
                     'Aucune valeur d’audience ne sera inventée.')

def standardize(rows,stamp,source):
    normalized=[]
    for item in rows:
        keys={str(k).lower().strip():v for k,v in item.items()}
        row={}
        for col,aliases in ALIASES.items():
            row[col]=next((keys[k] for k in aliases if k in keys and keys[k] is not None),'')
        row['collected_at']=stamp;row['source']=source
        normalized.append(row)
    out=pd.DataFrame(normalized,columns=COLUMNS)
    if out.empty:return out
    out['viewer_count']=pd.to_numeric(out['viewer_count'],errors='coerce')
    out=out[out.viewer_count.notna() & (out.viewer_count>=0)].copy()
    return out

def historical(session,all_files):
    api=f'https://api.github.com/repos/{REPO}/contents/'
    items=get_retry(session,api).json()
    files=sorted([x for x in items if re.fullmatch(r'20\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.txt',x['name'])],
                 key=lambda x:x['name'])
    if not all_files:files=files[:min(16,len(files))]
    print(f'Snapshots historiques détectés sur GitHub : {len(files)}')
    if not files:raise RuntimeError('Aucun snapshot daté disponible dans ce dépôt GitHub.')
    frames=[]
    for f in files:
        stamp=datetime.strptime(f['name'][:-4],'%Y-%m-%d-%H-%M').replace(tzinfo=timezone.utc).isoformat()
        try:
            content=get_retry(session,f['download_url']).content
            records=parse_text(content,f['name'])
            frame=standardize(records,stamp,'github_mingt2019_2017')
            if frame.empty:raise ValueError('Aucune audience valide')
            frames.append(frame)
            print(f"  {f['name']} : {len(frame)} observations")
        except (requests.RequestException,ValueError) as ex:
            print(f'  ÉCHEC {f["name"]} : {ex}')
    if not frames:raise RuntimeError('Aucun relevé historique exploitable téléchargé. Aucun historique inventé.')
    return pd.concat(frames,ignore_index=True)

def live(session,limit):
    cid=os.environ.get('TWITCH_CLIENT_ID');secret=os.environ.get('TWITCH_CLIENT_SECRET')
    if not cid or not secret:raise RuntimeError('Variables TWITCH_CLIENT_ID et TWITCH_CLIENT_SECRET absentes.')
    token=session.post('https://id.twitch.tv/oauth2/token',data={
        'client_id':cid,'client_secret':secret,'grant_type':'client_credentials'},timeout=30)
    token.raise_for_status()
    headers={'Client-Id':cid,'Authorization':'Bearer '+token.json()['access_token']}
    data={};cursor=None; stamp=datetime.now(timezone.utc).isoformat()
    while len(data)<limit:
        params={'first':min(100,limit-len(data))}
        if cursor:params['after']=cursor
        payload=get_retry(session,'https://api.twitch.tv/helix/streams',headers=headers,params=params).json()
        rows=payload.get('data',[])
        if not rows:break
        for r in rows:data[str(r['id'])]=r
        cursor=payload.get('pagination',{}).get('cursor')
        print(f'Helix : {len(data)} streams')
        if not cursor:break
    return standardize(list(data.values()),stamp,'helix_live')

def main():
    p=argparse.ArgumentParser(description='Fusion de snapshots Twitch historiques et actuels')
    p.add_argument('--limit',type=int,default=1000,help='nombre de streams live (0=historique seul)')
    p.add_argument('--historique-seul',action='store_true')
    p.add_argument('--tous-fichiers',action='store_true',help='tous les snapshots GitHub publiés')
    p.add_argument('--sortie',default='twitch_dataset_complet.csv')
    a=p.parse_args()
    if a.limit<0: p.error('--limit doit être positif')
    with requests.Session() as s:
        hist=historical(s,a.tous_fichiers)
        frames=[hist]
        if not a.historique_seul and a.limit:
            try:frames.append(live(s,a.limit))
            except (RuntimeError,requests.RequestException) as e:print('Helix indisponible :',e)
    result=pd.concat(frames,ignore_index=True)
    result['collected_at']=pd.to_datetime(result['collected_at'],utc=True,errors='coerce')
    result=result.dropna(subset=['collected_at','viewer_count'])
    result['viewer_count']=result['viewer_count'].astype('int64')
    result=result.drop_duplicates(subset=['source','id','user_id','collected_at'])
    dest=Path(a.sortie)
    if not dest.is_absolute():dest=ROOT/dest
    result.to_csv(dest,index=False,encoding='utf-8-sig')
    print('\nCSV créé :',dest)
    print('Lignes par source :\n'+result.groupby('source').size().to_string())
    print('Dates par source :\n'+result.groupby('source').collected_at.agg(['min','max']).to_string())
    print('IMPORTANT : les périodes historiques et actuelles ne sont pas continues.')

if __name__=='__main__':main()
