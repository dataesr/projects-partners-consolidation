import requests

def comparison(sources, Authorization):
    list_sources=[]
    for source in list(sources.keys()):
        if source == 'SIRANO':
            pass
        elif source == 'REG_IDF':
            response = requests.get(
                "https://data.iledefrance.fr/api/explore/v2.1/catalog/datasets/dim_map_projets_finances",
                headers={"Accept":"/"},
            )
            datas = response.json()
            date_projects=datas['metas']['default']['modified']
            
            response = requests.get(
                "https://data.iledefrance.fr/api/explore/v2.1/catalog/datasets/dim_map_projets_finances_entites_partenariat",
                headers={"Accept":"/"},
            )
            datas = response.json()
            date_partners=datas['metas']['default']['modified']
        else:
            response = requests.get(
                f"http://www.data.gouv.fr/api/1/datasets/?organization={sources[source]['id']}&format=json&q=dos",
                headers={"Accept":"/"},
            )
            datas = response.json()
            date_projects=[data for data in datas['data'][0]['resources'] if sources[source]['keyword_projects1'] in str(data['title']) !=-1 and sources[source]['keyword_projects2'] in str(data['title']) !=-1 ][0]['last_modified']
            date_partners=[data for data in datas['data'][0]['resources'] if sources[source]['keywords_partners1'] in str(data['title']) !=-1 and sources[source]['keywords_partners1'] in str(data['title']) !=-1][0]['last_modified']

        nbr_page=int(requests.get(f'http://185.161.45.213/projects/participations?where={"project_type":"{source}"}&projection={"modified_at":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])
        list_ids=[]
        for i in range(1,nbr_page+1):
            page=requests.get(f'http://185.161.45.213/projects/participations?where={"project_type":"{source}"}&projection={"modified_at":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
            for k in range(len(page['data'])):
                list_ids.append(page['data'][k]['modified_at'])
        max_date_partners=max(list_ids)

        nbr_page=int(requests.get(f'http://185.161.45.213/projects/projects?where={"type":"{source}"}&projection={"modified_at":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])
        list_ids=[]
        for i in range(1,nbr_page+1):
            page=requests.get(f'http://185.161.45.213/projects/projects?where={"type":"{source}"}&projection={"modified_at":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
            for k in range(len(page['data'])):
                list_ids.append(page['data'][k]['modified_at'])
        max_date_projects=max(list_ids)

        if max_date_partners<date_partners or max_date_projects<date_projects : 
            list_sources.append(source)
    return list_sources