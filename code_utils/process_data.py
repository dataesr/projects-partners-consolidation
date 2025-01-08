from dotenv import load_dotenv
import os
import pandas as pd
import requests 

from code_utils.cached_data_handler import get_structure, get_person
from code_utils.id_from_orcid import orcid_to_idref
from code_utils.pickle import load_cache, write_cache
from code_utils.utils import get_id,replace_all, get_scanR_structure

load_dotenv()
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
        print(df_partenaires.head())
        print(f"OK: {source} data have been successfully loaded - process_data (1/6)")
    except Exception as e:
        print(f"ERROR: {source} data have not been loaded", e)
        return None
    return df_partenaires

#find the identifier of each structure
def get_id_structure(df_partenaires, source, sources, cached_data):
    try:
        id_struct=df_partenaires
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
        df_partenaires[f"{sources[source]['nom_structure']}2"]=df_partenaires.loc[:,sources[source]['nom_structure']].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        df_partenaires_struct=pd.merge(df_partenaires,id_struct[[f"{sources[source]['nom_structure']}2",'id_structure_matcher']], on=f"{sources[source]['nom_structure']}2", how='left')
        print(df_partenaires_struct.head(), df_partenaires_struct.columns)
        print(f"OK: The matcher worked successfully on {source} structures - process_data (2/6)")
    except Exception as e:
        print(f"ERROR: The matcher didn't work on {source} structures", e)
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
        df_partenaires_struct=pd.merge(df_partenaires_struct,scanR_nettoye, on=f"{sources[source]['nom_structure']}2", how='left')
        print(df_partenaires_struct.head(), df_partenaires_struct.columns)
        print(f"OK: The ids from scanR are successfully added to {source} structures - process_data (3/6)")
    except Exception as e:
        print(f"ERROR: The ids from scanR are not added to {source} structures", e)
        return None
    #file with structure identifiers found manually ==> 'code'
    try:
        scanr_structures=pd.read_excel('scanr_partenaires_non_identifies.xlsx')
        scanr_structures[f"{sources[source]['nom_structure']}2"]=scanr_structures.loc[:,'Nom'].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
        scanr_structures=scanr_structures[[f"{sources[source]['nom_structure']}2",'code']]
        scanr_structures=scanr_structures.dropna().drop_duplicates(subset=f"{sources[source]['nom_structure']}2")
        df_partenaires_all_structure_id=pd.merge(df_partenaires_struct,scanr_structures, on=f"{sources[source]['nom_structure']}2", how='left')
        if 'finess' in list(df_partenaires_all_structure_id.columns):
            finess_siret=pd.read_json("finess_siret-siege.json")
            df_partenaires_all_structure_id=pd.merge(df_partenaires_all_structure_id,finess_siret,how='left', on='finess')
        df_partenaires_all_structure_id['id_structure']=df_partenaires_all_structure_id.apply(lambda row: get_id(row,sources[source]['identifiants_preferes_structure']), axis=1)
        df_partenaires_all_structure_id.to_json(f"./DATA/{source}/df_partenaires_id_structures.json")
        print(df_partenaires_all_structure_id.head(), df_partenaires_all_structure_id.columns)
        print(f"OK: The ids from the file with structure identifiers are successfully added to {source} structures - process_data (4/6)")
    except Exception as e:
        print(f"ERROR: The ids from the file with structure identifiers are not added to {source} structures", e)
        return None
    #save the structures without any id
    try:
        identifiants_a_remplir=df_partenaires_all_structure_id.loc[(pd.isna(df_partenaires_all_structure_id['id_structure']))|(str(df_partenaires_all_structure_id['id_structure'])=='None')|(str(df_partenaires_all_structure_id['id_structure'])=='nan')]
        identifiants_a_remplir=identifiants_a_remplir.drop_duplicates(subset=f"{sources[source]['nom_structure']}2")
        identifiants_a_remplir=identifiants_a_remplir.reset_index()
        del identifiants_a_remplir['index']
        identifiants_a_remplir.to_excel(f"./missing_ids_structures/partenaires_non_identifies_{source}.xlsx", index=False)
        print(df_partenaires_all_structure_id.head(), df_partenaires_all_structure_id.columns)
        print(f"OK: The missing ids are successfully added to the folder 'missing_id_structures' - process_data (5/6)")
    except Exception as e:
        print(f"ERROR: The missing ids are not added to the folder 'missing_id_structures'", e)
        return None
    return df_partenaires_all_structure_id 

#find the identifier of each person
def get_id_person(df_partenaires_complete, source, sources, cached_data_persons, cached_data_orcid):
    try:
        if len([x for x in ['nom', 'prenom'] if x in list(sources[source].keys())])==2:
            df_partenaires_complete['id_personne']=df_partenaires_complete.progress_apply(lambda row: get_person(row, cached_data_persons,sources[source]['nom'],sources[source]['prenom']), axis=1)
            write_cache(cached_data_persons,f"./DATA/{source}/caches/cached_data_persons.pkl")
            if sources[source]['id_ORCID'] in list(df_partenaires_complete.columns):
                df_partenaires_complete['idref_ORCID']=df_partenaires_complete.progress_apply(lambda row: orcid_to_idref(row,cached_data_orcid,sources[source]['id_ORCID'],Authorization_ORCID), axis=1)
                write_cache(cached_data_orcid,f"./DATA/{source}/caches/cached_data_orcid.pkl")
                df_partenaires_complete.to_json(f"./DATA/{source}/df_partenaires_id_personne_ORCID.json")
            else:
                df_partenaires_complete.to_json(f"./DATA/{source}/df_partenaires_id_personne.json")
        print(df_partenaires_complete.head(), df_partenaires_complete.columns)
        print(f"OK: The matcher worked successfully on {source} persons - process_data (6/6)")
    except Exception as e:
        print(f"ERROR: The matcher didn't work on {source} persons", e)
        return None
    return df_partenaires_complete


