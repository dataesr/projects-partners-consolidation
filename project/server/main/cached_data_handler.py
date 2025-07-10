import requests
from project.server.main.Pydref import Pydref
from retry import retry
import os
from dotenv import load_dotenv

load_dotenv()

#urls
url='https://affiliation-matcher.staging.dataesr.ovh/match'
url_cluster = os.getenv('url_cluster')

#get the structure id from the structure name
@retry(delay=200, tries=30000)
def get_structure(row,source,cached_data,nom_structure,pays,ville,code_projet,annee_input):
    if row[nom_structure] in list(cached_data.keys()):
        pass
    else:
        url='https://affiliation-matcher.staging.dataesr.ovh/match'
        f= ' '.join([str(row[y]) for y in [x for x in [nom_structure,ville,pays] if x in list(row.keys())]])
        print(f)
        if source=='REG_IDF':
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":str(row[code_projet].split('-')[3]),
                                           "query":f,"verbose":False})
        elif source not in ['REG_IDF','ADEME','SIRANO']:
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":"20"+str(row[code_projet].split('-')[1])[-2:],
                                           "query":f,"verbose":False})
        else:
            if source=='SIRANO':
                annee=row['annee_de_selection']
            else:
                annee=row[annee_input]
            rnsr=requests.post(url, json= {"type":"rnsr","year":str(annee),"query":f,"verbose":False})
        ror=requests.post(url, json= { "query" : f , "type":"ror"})
        grid=requests.post(url, json= { "query" : f , "type":"grid"})
        result_rnsr=rnsr.json()['results']
        result_ror=ror.json()['results']
        result_grid=grid.json()['results'] 
        print(result_rnsr,result_ror,result_grid)
        if result_rnsr != []:
            cached_data[row[nom_structure]]=result_rnsr
        elif result_rnsr != [] and result_grid != []:
            cached_data[row[nom_structure]]=result_grid
        elif result_rnsr != [] and result_grid == [] and result_ror != []:
            cached_data[row[nom_structure]]=result_ror
        else:
            cached_data[row[nom_structure]]=None

#get the structure id from the structure name
@retry(delay=200, tries=30000)
def get_person(row, cached_data_persons,prenom,nom):
    if f"{row[prenom]} {row[nom]}" in list(cached_data_persons.keys()):
        return cached_data_persons[f"{row[prenom]} {row[nom]}"]
    else:
        pydref = Pydref()
        result = pydref.identify(f"{row[prenom]} {row[nom]}")
        if result['status']=='found' and result['idref']!='idref073954012':
            cached_data_persons[f"{row[prenom]} {row[nom]}"]=result.get('idref')
            return result.get('idref')
        elif result['status']=='not_found_ambiguous':
            return f"{result['nb_homonyms']}_homonyms__not_found_ambiguous"
        else:
            return None    