from dotenv import load_dotenv
import os
import pandas as pd
import requests 
from tqdm import tqdm

from utils.cached_data_handler import get_structure, get_person
from utils.id_from_orcid import orcid_to_idref
from utils.logger import get_logger
from utils.pickle import load_cache, write_cache
from utils.utils import get_id,replace_all, get_scanR_structure

logger = get_logger(__name__)

load_dotenv()
tqdm.pandas()
Authorization_ORCID = os.getenv('Authorization_cluster_BSO_ORCID')

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

#load the data
def get_data(sources,source):
    try:
        if source=='ANR':
            page_partenaires_10 = requests.get(sources[source]['url_partners']).json()
            colonnes_partenaires_10 = page_partenaires_10['columns']
            donnees_partenaires_10 = page_partenaires_10['data']
            df_partners=pd.DataFrame(data=donnees_partenaires_10,columns=colonnes_partenaires_10)
        elif source=='ANSES':
            df_from_anses=pd.read_excel(sources[source]['url_partners'])
            df=df_from_anses.iloc[1:,:]
            df.columns=list(df_from_anses.iloc[0,:])
            dict_equipe={list(df_from_anses.columns)[k].replace('Équipe 10 ','Équipe 10').replace('Équipe13','Équipe 13'):k for k in range (len(list(df_from_anses.columns))) if list(df_from_anses.columns)[k].find('Équipe')>=0}
            list_df=[]
            number=3
            for n in range(1,len(dict_equipe)+1):
                equipe_n=pd.concat([df.iloc[:,0:3],df.iloc[:,number:number+6]], axis=1)
                list_df.append(equipe_n)
                number+=6
            df_partners=pd.concat([list_df[k].dropna(subset=[sources[source]['nom'], sources[source]['prenom'],sources[source]['nom_structure'], sources[source]['nom'], 'Pays'], how='all') for k in range(len(list_df))]) 
        elif source=='IRESP':
            df_partners1=pd.read_csv(sources[source]['url_partners1'] ,sep=";", encoding='UTF-8')
            df_partners2=pd.read_csv(sources[source]['url_partners2'] ,sep=";", encoding='UTF-8')
            df_partners=pd.concat([df_partners1,df_partners2])
        elif source=='ADEME':
            df_partners=pd.read_csv(sources[source]['url_partners'] ,sep=",", encoding='ISO-8859-1', on_bad_lines='skip')
        else:    
            df_partners=pd.read_csv(sources[source]['url_partners'] ,sep=";", encoding='ISO-8859-1')
        df_partners=df_partners.reset_index()
        del df_partners['index']
        print(df_partners.head())
        print(f"OK: {source} data have been successfully loaded - process_data (1/6)")
        logger.debug(f"OK: {source} data have been successfully loaded - process_data (1/6)")
    except Exception as e:
        print(f"ERROR: {source} data have not been loaded", e)
        logger.debug(f"ERROR: {source} data have not been loaded", e)
        return None
    return df_partners

#find the identifier of each structure
def get_id_structure(df_partners, source, sources, cached_data):
    try:
        id_struct=df_partners
        id_struct[f"{sources[source]['nom_structure']}2"]=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        id_struct=id_struct.drop_duplicates(subset=[f"{sources[source]['nom_structure']}2"])
        #apply the matcher to find structures ids
        id_struct.progress_apply(lambda row: get_structure(row,source,cached_data,sources[source]['nom_structure'],sources[source]['ville'],sources[source]['pays'],sources[source]['code_projet'],False), axis=1) 
        write_cache(cached_data,f"./DATA/{source}/caches/cached_{source}_data.pkl")
        id_struct['id_structure_matcher']=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: cached_data[x])
        id_struct=id_struct.reset_index()
        del id_struct['index']
        id_struct=id_struct[[sources[source]['nom_structure'],'id_structure_matcher']]
        id_struct[f"{sources[source]['nom_structure']}2"]=id_struct.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        df_partners[f"{sources[source]['nom_structure']}2"]=df_partners.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        df_partners_struct=pd.merge(df_partners,id_struct[[f"{sources[source]['nom_structure']}2",'id_structure_matcher']], on=f"{sources[source]['nom_structure']}2", how='left')
        print(df_partners_struct.head(), df_partners_struct.columns)
        print(f"OK: The matcher worked successfully on {source} structures - process_data (2/6)")
        logger.debug(f"OK: The matcher worked successfully on {source} structures - process_data (2/6)")
    except Exception as e:
        print(f"ERROR: The matcher didn't work on {source} structures", e)
        logger.debug(f"ERROR: The matcher didn't work on {source} structures", e)
        return None
    #find other identifiers with scanR
    try:
        url_scanr='https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/scanR/projects.json'
        requete_scanR = requests.get(url_scanr)
        page_scanR= requete_scanR.json()
        df_scanR=pd.DataFrame(page_scanR)
        scanR=df_scanR.explode('participants').loc[:,['id','participants']]
        scanR=scanR.rename(columns={'id':'id_anr'})
        scanR['index']=[x for x in range(len(scanR))]
        scanR=scanR.set_index('index')
        scanR['id_structure_scanr']=scanR['participants'].apply(lambda x: x.get(str('structure')) if isinstance(x, dict) else None )
        scanR['nom_struct']=scanR['participants'].apply(lambda x: get_scanR_structure(x))
        del scanR['participants']
        scanR_nettoye=scanR.drop_duplicates(subset='nom_struct')
        scanR_nettoye[f"{sources[source]['nom_structure']}2"]=scanR_nettoye.loc[:,'nom_struct'].apply(lambda x: replace_all(str(x).lower()))
        scanR_nettoye=scanR_nettoye[['id_structure_scanr',f"{sources[source]['nom_structure']}2"]]
        scanR_nettoye=scanR_nettoye.drop_duplicates(subset=f"{sources[source]['nom_structure']}2")
        df_partners_struct=pd.merge(df_partners_struct,scanR_nettoye, on=f"{sources[source]['nom_structure']}2", how='left')
        print(df_partners_struct.head(), df_partners_struct.columns)
        print(f"OK: The ids from scanR are successfully added to {source} structures - process_data (3/6)")
        logger.debug(f"OK: The ids from scanR are successfully added to {source} structures - process_data (3/6)")
    except Exception as e:
        print(f"ERROR: The ids from scanR are not added to {source} structures", e)
        logger.debug(f"ERROR: The ids from scanR are not added to {source} structures", e)
        return None
    #file with structure identifiers found manually ==> 'code'
    try:
        scanr_structures=pd.read_excel('scanr_partenaires_non_identifies.xlsx')
        scanr_structures[f"{sources[source]['nom_structure']}2"]=scanr_structures.loc[:,'Nom'].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        scanr_structures=scanr_structures[[f"{sources[source]['nom_structure']}2",'code']]
        scanr_structures=scanr_structures.dropna().drop_duplicates(subset=f"{sources[source]['nom_structure']}2")
        df_partners_all_structure_id=pd.merge(df_partners_struct,scanr_structures, on=f"{sources[source]['nom_structure']}2", how='left')
        if 'finess' in list(df_partners_all_structure_id.columns):
            finess_siret=pd.read_json("finess_siret-siege.json")
            df_partners_all_structure_id=pd.merge(df_partners_all_structure_id,finess_siret,how='left', on='finess')
        df_partners_all_structure_id['id_structure']=df_partners_all_structure_id.apply(lambda row: get_id(row,sources[source]['identifiants_preferes_structure']), axis=1)
        df_partners_all_structure_id.to_json(f"./DATA/{source}/df_partners_id_structures.json")
        print(df_partners_all_structure_id.head(), df_partners_all_structure_id.columns)
        print(f"OK: The ids from the file with structure identifiers are successfully added to {source} structures - process_data (4/6)")
        logger.debug(f"OK: The ids from the file with structure identifiers are successfully added to {source} structures - process_data (4/6)")
    except Exception as e:
        print(f"ERROR: The ids from the file with structure identifiers are not added to {source} structures", e)
        logger.debug(f"ERROR: The ids from the file with structure identifiers are not added to {source} structures", e)
        return None
    #save the structures without any id
    try:
        identifiants_a_remplir=df_partners_all_structure_id.loc[(pd.isna(df_partners_all_structure_id['id_structure']))|(str(df_partners_all_structure_id['id_structure'])=='None')|(str(df_partners_all_structure_id['id_structure'])=='nan')]
        identifiants_a_remplir=identifiants_a_remplir.drop_duplicates(subset=f"{sources[source]['nom_structure']}2")
        identifiants_a_remplir=identifiants_a_remplir.reset_index()
        del identifiants_a_remplir['index']
        identifiants_a_remplir.to_excel(f"./missing_ids_structures/partenaires_non_identifies_{source}.xlsx", index=False)
        print(df_partners_all_structure_id.head(), df_partners_all_structure_id.columns)
        print(f"OK: The missing ids are successfully added to the folder 'missing_id_structures' - process_data (5/6)")
        logger.debug(f"OK: The missing ids are successfully added to the folder 'missing_id_structures' - process_data (5/6)")
    except Exception as e:
        print(f"ERROR: The missing ids are not added to the folder 'missing_id_structures'", e)
        logger.debug(f"ERROR: The missing ids are not added to the folder 'missing_id_structures'", e)
        return None
    return df_partners_all_structure_id 

#find the identifier of each person
def get_id_person(df_partners_complete, source, sources, cached_data_persons, cached_data_orcid):
    try:
        if len([x for x in ['nom', 'prenom'] if x in list(sources[source].keys())])==2:
            df_partners_complete['id_personne']=df_partners_complete.progress_apply(lambda row: get_person(row, cached_data_persons,sources[source]['nom'],sources[source]['prenom']), axis=1)
            write_cache(cached_data_persons,f"./DATA/{source}/caches/cached_data_persons.pkl")
            if sources[source]['id_ORCID'] in list(df_partners_complete.columns):
                df_partners_complete['idref_ORCID']=df_partners_complete.progress_apply(lambda row: orcid_to_idref(row,cached_data_orcid,sources[source]['id_ORCID'],Authorization_ORCID), axis=1)
                write_cache(cached_data_orcid,f"./DATA/{source}/caches/cached_data_orcid.pkl")
                df_partners_complete.to_json(f"./DATA/{source}/df_partners_id_person_ORCID.json")
            else:
                df_partners_complete.to_json(f"./DATA/{source}/df_partners_id_person.json")
        print(df_partners_complete.head(), df_partners_complete.columns)
        print(f"OK: The matcher worked successfully on {source} persons - process_data (6/6)")
        logger.debug(f"OK: The matcher worked successfully on {source} persons - process_data (6/6)")
    except Exception as e:
        print(f"ERROR: The matcher didn't work on {source} persons", e)
        logger.debug(f"ERROR: The matcher didn't work on {source} persons", e)
        return None
    return df_partners_complete


