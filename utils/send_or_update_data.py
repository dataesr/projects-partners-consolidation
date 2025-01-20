import os
import pprint as pp
import requests

from dotenv import load_dotenv

from utils.formatting_data_partners import filter_new_partners
from utils.formatting_data_projects import filter_new_projects

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
def send_only_newer_data(df, url, type, source):
    if type == 'projects' :
        df_filtered=filter_new_projects(df, source)
        if len(df_filtered[0])!=0:
            print(len(df_filtered[0]))
            send_data(df_filtered[0], url)
            print(f"Add {len(df_filtered)} new projects successfully")
        else:
            print(f"No updates yet for {source}")
    else:
        df_filtered=filter_new_partners(df, source)
        print(len(df_filtered[0]))
        if len(df_filtered[0])>0:
            send_data(df_filtered[0], url)
            print(f"Add {len(df_filtered[0])} new partners succesfully")
        else:
            print(f"No updates yet for {source}")

#if we only need to make changes to specific features within projects
def edit_projects():
    print("faut que je demande quels champs changent d'une maj a une autre")

#if we only need to make changes to specific features within partners
def edit_partners():
    print("faut que je demande quels champs changent d'une maj a une autre")

