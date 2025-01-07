import requests
from retry import retry


#get idref from ORCID
@retry(delay=200, tries=30000)
def orcid_to_idref(row,cached_data_orcid,id_ORCID,Authorization_ORCID):
    if row[id_ORCID] in list(cached_data_orcid.keys()):
        return cached_data_orcid[row[id_ORCID]]
    else:
        orcid=row[id_ORCID]
        url=f'https://cluster-production.elasticsearch.dataesr.ovh/bso-orcid-20231024/_search?q=orcid:"{orcid}"'
        res = requests.get(url, headers={'Authorization': Authorization_ORCID}).json()
        if res['hits']['hits']!=[]:
            if 'idref_abes' in list(res['hits']['hits'][0]['_source'].keys()):
                if res['hits']['hits'][0]['_source']['idref_abes']!=None:
                    cached_data_orcid[row[id_ORCID]]=res['hits']['hits'][0]['_source']['idref_abes']
                    return res['hits']['hits'][0]['_source']['idref_abes']
                else:
                    return None
            else:
                return None
        else:
            return None