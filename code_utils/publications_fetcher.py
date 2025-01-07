import requests

# get the publications and their dates from the first and last names of participants
def get_from_es(url,body):
    return requests.post(url, json=body, verify=False).json()

def get_data_from_elastic(url,fullname):
    body = {
        'size': 10,
        'query': {
            'bool': {
                'filter': [
                    {'term': {'fullName.keyword': fullname}}
                ]
            }
        },
        '_source':{
            'excludes'
            : 
            ["publications", "projects", "web_content", "patents", "autocompleted", "autocompletedText"]},
        'track_total_hits': True
    }
    res = get_from_es(url,body)
    return res