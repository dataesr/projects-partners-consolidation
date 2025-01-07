import pandas as pd
import requests 

from code_utils.cached_data_handler import get_structure, get_person
from code_utils.pickle import load_cache,write_cache
from code_utils.utils import extract_first_name,replace_all,get_scanR_structure

def cache(source):
    cached_data = {}
    try:
        cached_data = load_cache(cached_data,f"./DATA/{source}/caches/cached_{source.lower()}_data.pkl")
    except:
        write_cache(cached_data,f"./DATA/{source}/caches/cached_{source.lower()}_data.pkl")
        
    cached_data_persons = {}
    try:
        cached_data_persons = load_cache(cached_data_persons,f"./DATA/{source}/caches/cached_{source.lower()}_data_persons.pkl")
    except:
        write_cache(cached_data_persons,f"./DATA/{source}/caches/cached_{source.lower()}_data_persons.pkl")
        
    cached_data_orcid = {}
    try:
        cached_data_orcid = load_cache(cached_data_orcid,f"./DATA/{source}/caches/cached_{source.lower()}_data_orcid.pkl")
    except:
        write_cache(cached_data_orcid,f"./DATA/{source}/caches/cached_{source.lower()}_data_orcid.pkl")

    return cached_data, cached_data_persons, cached_data_orcid

def get_data(sources,source):
    if source=='ANR':
        page_partenaires_10 = requests.get(sources[source]['url_partenaires']).json()
        colonnes_partenaires_10 = page_partenaires_10['columns']
        donnees_partenaires_10 = page_partenaires_10['data']
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

    df_partenaires=df_partenaires.reset_index()
    del df_partenaires['index']
    return df_partenaires

def get_id_structure_matcher(df_partenaires, source, sources, cached_data):
    id_struct=df_partenaires
    id_struct[f"{sources[source]['nom_structure']}2"]=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
    id_struct=id_struct.drop_duplicates(subset=[f"{sources[source]['nom_structure']}2"])
    id_struct.progress_apply(lambda row: get_structure(row,source,cached_data,sources[source]['nom_structure'],sources[source]['ville'],sources[source]['pays'],sources[source]['code_projet'],False), axis=1) 
    write_cache(cached_data,f"./DATA/{source}/caches/cached_{source}_data.pkl")
    id_struct['id_structure_matcher']=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: cached_data[x])
    id_struct=id_struct.reset_index()
    del id_struct['index']
    id_struct.to_json(f"./DATA/{source}/df_partenaires.json")
    id_struct=pd.read_json(f"./DATA/{source}/df_partenaires.json")
    id_struct=id_struct[[sources[source]['nom_structure'],'id_structure_matcher']]
    id_struct[f"{sources[source]['nom_structure']}2"]=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
