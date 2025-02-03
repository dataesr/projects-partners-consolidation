from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import requests
import sys

from features_into_dictionnary import address

load_dotenv()
Authorization = os.getenv('Authorization_access_185')

def formatting_partners_data(sources, source):
    df_partners=pd.read_json(f"./DATA/{source}/df_partners_id_structures.json")
    ### ATTENTION, vérifier que les projets sirano sont dans des structures françaises
    if source=='IRESP':
        df_partners[sources[source]['pays']]=df_partners.loc[:,sources[source]['ville']].apply(lambda x: x.split('(')[1].replace(')','') if x.find('(')>=0 else 'France')
        df_partners.loc[:,sources[source]['ville']]=df_partners.loc[:,sources[source]['ville']].apply(lambda x: x.split('(')[0] if x.find('(')>=0 else x)
    
    df_partners['address']=df_partners.apply(lambda row: address(row,sources[source]['pays'],sources[source]['ville'],source), axis=1)
    df_partners.loc[:,'id_structure']=df_partners.loc[:,'id_structure'].apply(lambda x: x[0] if isinstance(x,list) else x )
    if source in ['ANSES','SIRANO']:
        df_partners['id']=df_partners.apply(lambda row: f"{row[sources[source]['code_projet']]}-{row[str(sources[source]['nom_structure'])+'2']}-{row[sources[source]['nom']]}-{row[sources[source]['prenom']]}" , axis=1)
    if source =='REG_IDF':
        df_partners['id']=df_partners.apply(lambda row: f"{row[sources[source]['code_projet']]}-{row[str(sources[source]['nom_structure'])+'2']}-{row['entite_role']}" , axis=1)
    df_partners['address']=df_partners.apply(lambda row: address(row,sources[source]['pays'],sources[source]['ville'],source), axis=1)
    df_partners=df_partners.rename(columns={sources[source]['nom_structure']: 'name', sources[source]['code_projet']: 'project_id', 'id_structure':'participant_id','Projet.Partenaire.Code_Decision_ANR':'id'})
    df_partners=df_partners[['name','id','project_id','participant_id','address']]
    df_partners['project_type']=source
    df_partners['participant_id']=df_partners.loc[:,'participant_id'].apply(lambda x: str(x[0]).replace('.0','') if isinstance(x,list) else str(x).split(';')[0].replace('.0',''))
    df_partners=df_partners[['id','project_id', 'project_type', 'participant_id', 'name','address']]
    df_partners['name'] = df_partners['name'].astype(str)
    print(f"There is no duplicated id :{len(df_partners[df_partners.duplicated(subset=['id'])])==0}")
    return df_partners

def filter_new_partners(df_partners, source):
    nbr_page=int(requests.get('http://185.161.45.213/projects/participations?where={"project_type":"'+str(source)+'"}&projection={"id":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])
    list_ids=[]
    for i in range(1,nbr_page+1):
        print("page",i)
        page=requests.get('http://185.161.45.213/projects/participations?where={"project_type":"'+str(source)+'"}&projection={"id":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
        for k in range(len(page['data'])):
            print("k",k)
            list_ids.append(page['data'][k]['id'])    
    partners_to_add=[x for x in list(df_partners['id'].drop_duplicates()) if x not in list(pd.Series(list_ids).drop_duplicates())]
    partners_to_remove=[x for x in list_ids if x not in list(df_partners['id'])]
    df_partners = df_partners[df_partners['id'].apply(lambda x: x in partners_to_add)]
    return df_partners, partners_to_add, partners_to_remove