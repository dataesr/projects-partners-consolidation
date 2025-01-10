import pandas as pd

from code_utils.features_into_dictionnary import address

def formatting_partners_data(sources, source):
    df_partenaires=pd.read_json(f"./DATA/{source}/df_partenaires_id_structures.json")
    ### ATTENTION, vérifier que les projets sirano sont dans des structures françaises
    if source=='IRESP':
        df_partenaires[sources[source]['pays']]=df_partenaires.loc[:,sources[source]['ville']].apply(lambda x: x.split('(')[1].replace(')','') if x.find('(')>=0 else 'France')
        df_partenaires.loc[:,sources[source]['ville']]=df_partenaires.loc[:,sources[source]['ville']].apply(lambda x: x.split('(')[0] if x.find('(')>=0 else x)
    
    df_partenaires['address']=df_partenaires.apply(lambda row: address(row,sources[source]['pays'],sources[source]['ville'],source), axis=1)
    df_partenaires.loc[:,'id_structure']=df_partenaires.loc[:,'id_structure'].apply(lambda x: x[0] if isinstance(x,list) else x )
    if source in ['ANSES','SIRANO']:
        df_partenaires['id']=df_partenaires.apply(lambda row: f"{row[sources[source]['code_projet']]}-{row[{sources[source]['nom_structure']}+'2']}-{row[sources[source]['nom']]}-{row[sources[source]['prenom']]}" , axis=1)
    if source =='REG_IDF':
        df_partenaires['id']=df_partenaires.apply(lambda row: f"{row[sources[source]['code_projet']]}-{row[str(sources[source]['nom_structure'])+'2']}-{row['entite_role']}" , axis=1)
    df_partenaires['address']=df_partenaires.apply(lambda row: address(row,sources[source]['pays'],sources[source]['ville'],source), axis=1)
    df_partenaires=df_partenaires.rename(columns={sources[source]['nom_structure']: 'name', sources[source]['code_projet']: 'project_id', 'id_structure':'participant_id','Projet.Partenaire.Code_Decision_ANR':'id'})
    df_partenaires=df_partenaires[['name','id','project_id','participant_id','address']]
    df_partenaires['project_type']=source
    df_partenaires['participant_id']=df_partenaires.loc[:,'participant_id'].apply(lambda x: str(x[0]).replace('.0','') if isinstance(x,list) else str(x).split(';')[0].replace('.0',''))
    df_partenaires=df_partenaires[['id','project_id', 'project_type', 'participant_id', 'name','address']]
    df_partenaires['name'] = df_partenaires['name'].astype(str)