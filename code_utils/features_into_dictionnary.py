import pandas as pd


#get a dictionnary for persons like : {"id":"idref", "first_name":"prénom", "last_name":"nom" }
def persons(row, prenom, nom):
    if pd.isna(row['id_structure'])==False and row['id_structure']!='x':
        dict_row={"id" : row['id_person'], "first_name": row[prenom], "last_name": row[nom], "role":f"scientific-officer###{str(row['id_structure'])}"}
    else:
        dict_row={"id" : row['id_person'], "first_name": row[prenom], "last_name": row[nom], "role":"scientific-officer"}
    dict_row2={k:v for k,v in dict_row.items() if (pd.isna(v)==False)}
    return dict_row2

#get a dictionnary for projects like : {"fr": "titre ou résumé en français", "en": "titre ou résumé en anglais"}
def projects(row,champ_fr,champ_en):
    if champ_en in list(row.keys()):
        if pd.isna(row[champ_fr])==False and pd.isna(row[champ_en])==False:
            return {"fr": row[champ_fr], "en": row[champ_en]}
        elif pd.isna(row[champ_fr]) and pd.isna(row[champ_en])==False:
            return {"en": row[champ_en]}
        elif pd.isna(row[champ_fr])==False and pd.isna(row[champ_en]):
            return {"fr": row[champ_fr]}
        else:
            return None
    elif champ_en not in list(row.keys()) and champ_fr in list(row.keys()):
        if pd.isna(row[champ_fr])==False:
            return {"fr": row[champ_fr]}
        else:
            return None
    else:
        return None
    
#get a dictionnary for structures localisation like : {"city": "the city of the structure", "country": "the country of the structure"}
def address(row,pays,ville,source):
    if source == 'SIRANO':
        return {"city": "France"}
    else:
        if (pd.isna(row[ville])==False)&(pd.isna(row[pays])==False):
            return {"city": row[ville], "country": row[pays]}
        elif (pd.isna(row[ville]))&(pd.isna(row[pays])==False):
            return {"country": row[pays]}
        elif (pd.isna(row[ville])==False)&(pd.isna(row[pays])):
            return {"city": row[ville]}
        else:
            return None