import os
import pprint as pp
import requests

from dotenv import load_dotenv

from code_utils.formatting_data_partners import filter_new_partners
from code_utils.formatting_data_projects import filter_new_projects

load_dotenv()
Authorization = os.getenv('Authorization_access_185')

#if it is a new source of data
def send_data(df, url):
    err=[]
    for i,row in df.iterrows():
        dict_row=row.to_dict()
        dict_row2={k:v for k,v in list(dict_row.items()) if ((str(v)!='nan')&(str(v)!='NaN')&(str(v)!='None')&(str(v)!='x')&(str(v)!='[]'))}
        try:
            r=requests.post(url, json = dict_row2, headers={"Authorization":Authorization})
            res= r.json()
            if res.get('status')=='ERR':
                err.append(res)
                if res.get('error').get('code')!=422:
                    print(err)
                    pp.pprint(err)
        except Exception as e:
            pp.pprint(e)

#if we only need to update the new projects or partners
def send_only_newer_data(df, url, type):
    if type == 'projects' :
        send_data(filter_new_projects(df), url)
    else:
        send_data(filter_new_partners(df), url)

#if we only need to make changes to specific features within projects
def edit_projects():
    print("faut que je demande quels champs changent d'une maj a une autre")

#if we only need to make changes to specific features within partners
def edit_projects():
    print("faut que je demande quels champs changent d'une maj a une autre")

