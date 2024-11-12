from flask import Flask, request, jsonify, render_template
import requests
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from tqdm import tqdm
from code_utils.functions import (
    extraire_prenom, get_data_from_elastic, address, champs_dict, 
    identifie_structure, identifie_personne, identifiant_prefere, 
    replace_all, nettoie_scanR, orcid_to_idref, persons
)
from code_utils.pickle import load_cache, write_cache

app=Flask(__name__)
tqdm.pandas()
load_dotenv()

Authorization=os.getenv('Authorization_access_185.XX')
Authorization_ORCID=os.getenv('Authorization_cluster_BSO_ORCID')
url_cluster=os.getenv('url_cluster')

sources=pd.read_json('sources.json')

@app.route('/', methods=['GET'])
def select_source():
    return render_template('select_source.html')

@app.route('/index', methods=['POST'])
def process_data():
    if request.method == 'POST':
        source = request.form.get('source')
        if not source:
            return jsonify({"error": "No source selected"}), 400
    else: 
        return jsonify({"error": "Invalid method, only POST allowed"}), 405

    if source not in sources.keys():
        return jsonify({"error": "No source selected"}), 400

    cached_data, cached_data_persons, cached_data_orcid=load_or_create_caches(source)
    
    df_partenaires=get_data(source)
    df_partenaires_struct=get_data_with_structures(df_partenaires, source, cached_data)
    df_partenaires_complet=complete_with_scanr(df_partenaires_struct, source)
    identifiants_a_remplir=get_missing_identifiers(df_partenaires_complet, source)

    return jsonify({"processed_data": identifiants_a_remplir.to_dict()})


def load_or_create_caches(source):
    cached_data={}
    try:
        cached_data=load_cache(cached_data, f"./DATA/{source}/caches/cached_{source.lower()}_data.pkl")
    except:
        write_cache(cached_data, f"./DATA/{source}/caches/cached_{source.lower()}_data.pkl")
        
    cached_data_persons={}
    try:
        cached_data_persons=load_cache(cached_data_persons, f"./DATA/{source}/caches/cached_{source.lower()}_data_persons.pkl")
    except:
        write_cache(cached_data_persons, f"./DATA/{source}/caches/cached_{source.lower()}_data_persons.pkl")

    cached_data_orcid={}
    try:
        cached_data_orcid=load_cache(cached_data_orcid, f"./DATA/{source}/caches/cached_{source.lower()}_data_orcid.pkl")
    except:
        write_cache(cached_data_orcid, f"./DATA/{source}/caches/cached_{source.lower()}_data_orcid.pkl")
    
    return cached_data, cached_data_persons, cached_data_orcid


def get_data(source):
    if source=='ANR':
        page_partenaires_10=requests.get(sources[source]['url_partenaires']).json()
        colonnes_partenaires_10=page_partenaires_10['columns']
        donnees_partenaires_10=page_partenaires_10['data']
        df_partenaires=pd.DataFrame(data=donnees_partenaires_10,columns=colonnes_partenaires_10)
    elif source=='ANSES':
        df_from_anses=pd.read_excel(sources[source]['url_partenaires'])
        df=df_from_anses.iloc[1:,:]
        df.columns=list(df_from_anses.iloc[0,:])
        dict_equipe={list(df_from_anses.columns)[k].replace('Équipe 10 ','Équipe 10').replace('Équipe13','Équipe 13'):k for k in range (len(list(df_from_anses.columns))) if list(df_from_anses.columns)[k].find('Équipe')>=0}
        list_df=[]
        number=3
        for n in range(1,len(dict_equipe)+1):
            equipe_n=pd.concat([df.iloc[:,0:3],df.iloc[:,number:number+6]], axis=1)
            list_df.append(equipe_n)
            number+=6
        df_partenaires=pd.concat([list_df[k].dropna(subset=[sources[source]['nom'], sources[source]['prenom'],sources[source]['nom_structure'], sources[source]['nom'], 'Pays'], how='all') for k in range(len(list_df))])
    elif source=='IRESP':
        df_partenaires1=pd.read_csv(sources[source]['url_partenaires1'] ,sep=";", encoding='UTF-8')
        df_partenaires2=pd.read_csv(sources[source]['url_partenaires2'] ,sep=";", encoding='UTF-8')
        df_partenaires=pd.concat([df_partenaires1,df_partenaires2])
    elif source=='ADEME':
        df_partenaires=pd.read_csv(sources[source]['url_partenaires'] ,sep=",", encoding='ISO-8859-1', on_bad_lines='skip')
    else:    
        df_partenaires=pd.read_csv(sources[source]['url_partenaires'] ,sep=";", encoding='ISO-8859-1')
    return df_partenaires


def get_data_with_structures(df_partenaires, source, cached_data):
    df_partenaires[f"{sources[source]['nom_structure']}2"]=df_partenaires[sources[source]['nom_structure']].apply(
        lambda x: replace_all(str(x).lower())
    )
    df_partenaires_struct=df_partenaires.copy()
    df_partenaires_struct.progress_apply(lambda row: identifie_structure(row, source, cached_data, sources[source]['nom_structure'], sources[source]['ville'], sources[source]['pays'], sources[source]['code_projet'], False), axis=1)
    return df_partenaires_struct


def complete_with_scanr(df_partenaires_struct, source):
    url_scanr='https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/scanR/projects.json'
    page_scanR=requests.get(url_scanr).json()
    df_scanR=pd.DataFrame(page_scanR)
    scanR=df_scanR.explode('participants').loc[:, ['id', 'participants']]
    scanR['id_structure_scanr']=scanR['participants'].apply(lambda x: x.get('structure') if isinstance(x, dict) else None)
    scanR['nom_struct']=scanR['participants'].apply(lambda x: nettoie_scanR(x))
    scanR_nettoye=scanR.drop_duplicates(subset='nom_struct')
    scanR_nettoye[f"{sources[source]['nom_structure']}2"]=scanR_nettoye['nom_struct'].apply(lambda x: replace_all(str(x).lower()))
    return pd.merge(df_partenaires_struct, scanR_nettoye, on=f"{sources[source]['nom_structure']}2", how='left')


def get_missing_identifiers(df_partenaires_complet, source):
    missing=df_partenaires_complet[df_partenaires_complet['id_structure'].isna()]
    return missing.drop_duplicates(subset=f"{sources[source]['nom_structure']}2")


if __name__ == '__main__':
    app.run(debug=True)
