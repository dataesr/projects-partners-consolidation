from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
import requests
import sys

from features_into_dictionnary import persons, projects
from utils import get_id

load_dotenv()
Authorization = os.getenv('Authorization_access_185')

def formatting_projects_data(sources, source):
    #access partner data that has already been processed
    if len(sources[source]['identifiants_preferes_personne'])==2:
        df_partners=pd.read_json(f"./DATA/{source}/df_partners_id_person_ORCID.json")
    elif len(sources[source]['identifiants_preferes_personne'])==1:
        df_partners=pd.read_json(f"./DATA/{source}/df_partners_id_person.json")
    else:
        df_partners=pd.read_json(f"./DATA/{source}/df_partners_id_structures.json")
    df_partners.loc[df_partners.id_structure.apply(lambda x :isinstance(x,list)),'id_structure']=df_partners.loc[df_partners.id_structure.apply(lambda x :isinstance(x,list)),'id_structure'].apply(lambda y: y[0])
    if len([x for x in ['nom', 'prenom'] if x in list(sources[source].keys())])==2:
        df_partners['id_person']=df_partners.apply(lambda row: get_id(row,sources[source]['identifiants_preferes_personne']), axis=1)
        df_partners['persons']=df_partners.progress_apply(lambda row: persons(row,sources[source]['prenom'],sources[source]['nom']) ,axis=1)
    else:
        df_partners['persons']=np.nan
    if source != 'SIRANO':
        df_partners=df_partners.groupby([sources[source]['code_projet']]).agg({'persons': lambda x: [ y for y in x.tolist() if pd.isna(y)==False]}, dropna=False).reset_index()
    else:
        df_projects=df_partners.groupby([sources[source]['code_projet'], sources[source]['annee'], sources[source]['acronyme'],sources[source]['titre'],sources[source]['budget']], dropna=False).agg({'persons': lambda x: [ y for y in x.tolist() if pd.isna(y)==False]}, dropna=False)
    #bring projects from the website 
    if source=='ANR':
        page_projects_10 = requests.get(sources[source]['url_projects']).json()
        colonnes_projects_10 = page_projects_10['columns']
        donnees_projects_10 = page_projects_10['data']
        df_projects=pd.DataFrame(data=donnees_projects_10,columns=colonnes_projects_10)
    elif source=='IRESP':
        df_projects1=pd.read_csv(sources[source]['url_projects1'] ,sep=";", encoding='UTF-8')
        df_projects2=pd.read_csv(sources[source]['url_projects2'] ,sep=";", encoding='UTF-8')
        df_projects=pd.concat([df_projects1,df_projects2])
        df_projects.loc[pd.isna(df_projects['Titre_du_projet_FR']),'Titre_du_projet_FR']=df_projects.loc[pd.isna(df_projects['Titre_du_projet_FR']),'Titre_du_projet']
    elif source!='SIRANO':
        df_projects=pd.read_csv(sources[source]['url_projects'] ,sep=";", encoding='ISO-8859-1')
    df_projects=df_projects.reset_index()
    del df_projects['index']
    #join projects data and partners data to make a link between the project and all people working on it
    if source!='SIRANO':
        df_projects=pd.merge(df_projects,df_partners,on=sources[source]['code_projet'], how='left')
    else :
        df_projects['id']=df_partners.apply(lambda row: f"{row[sources[source]['code_projet']]}-{row[sources[source]['annee']]}-{row[sources[source]['acronyme']]}" , axis=1)
        del df_projects['code_projet']
        sources[source]['code_projet']='id'
    #rename the features 
    df_projects['type']=source
    df_projects['name']=df_projects.progress_apply(lambda row: projects(row,sources[source]['titre_fr'],sources[source]['titre_en']) ,axis=1)
    df_projects['description']=df_projects.progress_apply(lambda row: projects(row,sources[source]['resume_fr'],sources[source]['resume_en']) ,axis=1)
    df_projects.loc[:,sources[source]['budget']]=df_projects.loc[:,sources[source]['budget']].apply(lambda x : float(str(x).replace('.0','').replace('.00','').replace(' ','').replace(',','.').replace('€','').replace('\x80','')))
    df_projects=df_projects.rename(columns={sources[source]['annee']:'year',sources[source]['acronyme']:'acronym',
                                        sources[source]['budget']:'budget_financed',sources[source]['code_projet']:'id'})
    df_projects=df_projects[['id','type','name','description','acronym','year','budget_financed','persons']]
    return df_projects

def filter_new_projects(df_projects, source):
    nbr_page=int(requests.get('http://185.161.45.213/projects/projects?where={"type":"'+str(source)+'"}&projection={"id":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])
    list_ids=[]
    for i in range(1,nbr_page+1):
        print("page",i)
        page=requests.get('http://185.161.45.213/projects/projects?where={"type":"'+str(source)+'"}&projection={"id":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
        for k in range(len(page['data'])):
            print("k",k)
            list_ids.append(page['data'][k]['id'])
    projects_to_add=[x for x in list(df_projects['id']) if x not in list_ids]
    projects_to_remove=[x for x in list_ids if x not in list(df_projects['id'])]
    df_projects = df_projects[df_projects['id'].apply(lambda x: x in projects_to_add)]
    return df_projects, projects_to_add, projects_to_remove

