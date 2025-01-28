import requests
from utils.Pydref import Pydref
from retry import retry
import os
from dotenv import load_dotenv

from utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

#urls
url='https://affiliation-matcher.staging.dataesr.ovh/match'
url_cluster = os.getenv('url_cluster')

#get the structure id from the structure name
@retry(delay=200, tries=30000)
def get_structure(row,source,cached_data,nom_structure,pays=False,ville=False,code_projet=False,annee=False):
    if row[nom_structure] in list(cached_data.keys()):
        pass
    else:
        url='https://affiliation-matcher.staging.dataesr.ovh/match'
        f= ' '.join([str(row[y]) for y in [x for x in [nom_structure,ville,pays] if x in list(row.keys())]])
        if source=='REG_IDF':
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":str(row[code_projet].split('-')[3]),
                                           "query":f,"verbose":False})
        elif (annee == False)&(source not in ['REG_IDF','ADEME']):
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":"20"+str(row[code_projet].split('-')[1])[-2:],
                                           "query":f,"verbose":False})
        else:
            if source=='ADEME':
                annee=row['Date de dÃ©but du projet'][:4]
            else:
                annee=row[annee]
            rnsr=requests.post(url, json= {"type":"rnsr","year":str(annee),"query":f,"verbose":False})
        ror=requests.post(url, json= { "query" : f , "type":"ror"})
        grid=requests.post(url, json= { "query" : f , "type":"grid"})
        result_rnsr=rnsr.json()['results']
        result_ror=ror.json()['results']
        result_grid=grid.json()['results'] 
        if result_rnsr != []:
            cached_data[row[nom_structure]]=result_rnsr
        elif result_rnsr != [] and result_grid != []:
            cached_data[row[nom_structure]]=result_grid
        elif result_rnsr != [] and result_grid == [] and result_ror != []:
            cached_data[row[nom_structure]]=result_ror
        else:
            cached_data[row[nom_structure]]=None

#get the structure id from the structure name
#@retry(delay=200, tries=30000)
def get_person(row, cached_data_persons,prenom,nom):
    logger.debug(f"{row[prenom]} {row[nom]}")
    if f"{row[prenom]} {row[nom]}" in list(cached_data_persons.keys()):
        logger.debug('cache')        
        return cached_data_persons[f"{row[prenom]} {row[nom]}"]
    else:
        logger.debug('pydref')        
        pydref = Pydref()
        result = pydref.identify(f"{row[prenom]} {row[nom]}")
        if result['status']=='found' and result['idref']!='idref073954012':
            cached_data_persons[f"{row[prenom]} {row[nom]}"]=result.get('idref')
            return result.get('idref')
        elif result['status']=='not_found_ambiguous':
            return f"{result['nb_homonyms']}_homonyms__not_found_ambiguous"
        else:
            return None    