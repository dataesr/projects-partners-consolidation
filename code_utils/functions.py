import requests
import numpy as np
from code_utils.Pydref import Pydref
from retry import retry
import requests
import pandas as pd
#url matcher structure
url='https://affiliation-matcher.staging.dataesr.ovh/match'


#fonction identifie structure
@retry(delay=200, tries=30000)
def identifie_structure(row,source,cached_data,nom_structure,pays,ville=False,code_projet=False,annee=False):
    if row[nom_structure] in list(cached_data.keys()):
        pass
    else:
        url='https://affiliation-matcher.staging.dataesr.ovh/match'
        f= ' '.join([row[y] for y in [x for x in [nom_structure,ville,pays] if x in list(row.keys())]])
        if source=='REG_IDF':
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":str(row[code_projet].split('-')[3]),
                                           "query":f,"verbose":False})
        elif (annee == False)&(source!='REG_IDF'):
            rnsr=requests.post(url, json= {"type":"rnsr",
                                           "year":"20"+str(row[code_projet].split('-')[1])[-2:],
                                           "query":f,"verbose":False})
        else:
            rnsr=requests.post(url, json= {"type":"rnsr","year":str(row[annee]),"query":f,"verbose":False})
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

#fonction identifie personne
@retry(delay=200, tries=30000)
def identifie_personne(row, cached_data_persons,prenom,nom):
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
 
#fonction qui nettoie le nom de chaque structure sur scnaR
def nettoie_scanR(x):
    if (isinstance(x, dict)):
        if pd.isna(x['label'].get('default'))!=True:
            return x['label'].get('default').split('__-__')[0]
        else:
            return None
    else:
        return None

#fonction qui hiérarchise les identifiants selon la préférance
    
def identifiant_prefere(row,colonnes):
    for col in colonnes:
        if str(row[col])!='None' and str(row[col])!='NaN' and row[col] is not np.nan and str(row[col])[2:10]!='homonyms' and str(row[col])[3:11]!='homonyms':
            return row[col]
    return None

#fonction pour donner un identifiant a ceux qui en ont pas

def attribue_id(row,df):
    for i in range (len(df)):
        if row['Projet.Partenaire.Nom_organisme2']==list(df.loc[:,'Projet.Partenaire.Nom_organisme2'])[i] and (df.loc[i,'id_structure'] is np.nan or str(df.loc[i,'id_structure']) == 'None' or str(df.loc[i,'id_structure'] == 'NaN')):
            row['id_structure']=df.loc[i,'Projet.Partenaire.Nom_organisme2']
        else:
            row['id_structure']= None
        
#fonction qui aplatit une liste de listes:
def aplatir(conteneurs):
    return [conteneurs[i][j] for i in range(len(conteneurs)) for j in range(len(conteneurs[i]))]
  
#fonction qui remplace 

dic={" - japon":"","(vub)":"","d'hebron":""," (south africa)":"",
     "university of wageningen / biochemistry":"univeritywageningen"," - suède":"",
     " upv/ehu":"","rome la sapienza":"romaapienza","pensylvania":"penylvania",
     "isamail":"ismail","(unimib)":"","goteborg":"gothenburg","eastern finlande":"eaternfinland",
     "copenhaguen":"copenhague","colombia":"columbia","bayereuth":"bayreuth",
     "stendhal grenoble iii":"tendhalgrenoble3","université grenoble 1":"univeritegrenoblei",
     "essone":"eonne"," d’":"","montpelleir":"montpellier","lisbone":"libonne","ferrand 1":"ferrand",
     "diop de dakar":"diop","polite`cnica":"politecnica","polite`cnica":"politecnica",
     " milano":"","(ucsc)":"","(upv/ehu)":"","(ungda)":"","mannar":"manar","¨":""," sarl":"","(sgn)":"",
     "(sruc)":"","sapienza università di roma":"apienzauniverit?diroma"," cree:":"",": london":"",
     "(nioz)":"","de l' est (ppe)":"etppe","(necs)":"","veterinay":"veterinary","inst.":"intitute"," torun":"",
     "research and development":"rd"," -imnr":"","rresearch":"reearch","(rivm)":""," netherlands":"",
     " heath ":"health","for cell biology & genetics":""," für ":"","chaft zur":"",
     "universität universität münchen":"univeritymunich","(ldo)":"","(licsen)":"","envirionnement":"environnement",
     " ag: allemagne":"ag","kbs":"kedgebuinechool","für technologie - allemagne":"furtechnologie",
     "jozef stefan institut":"jozeftefanintitute",": instituto superior técnico":"","(ibet): portugal":"","(ist austria)":"",
     "(iciq) - espagne":"tarragona","institute national de ":"intitutnational"," (irb barcelona)":"",
     "institute for quantum optics and quantum information of the austrian academy of sciences":"intitutequantumopticquantuminformationautrianacamyciencevienna",
     "bioenginnering":"bioenginering","(ilvo)":"","(ipc)":""," nationall ":"nationale"," (irstea)":"",
     "institut national de recherche en sciences et technologies pour lenvironnement et de lagriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "institut national de recherche en sciences et technologies de lenvironnement et de lagriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "institut national de recherche en sciences et technoligies pour l'environnement et l'agriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "l'infromation":"linformation"," du ":"","institut français du textile et de l'habillement de paris":"intitutfrancaidutextileethabillement",
     "usr 3337 amérique latine":"ur3337","institut economie scientifique gest":"intituteconomiecientifiquegetionieeg",
     "doptique":"optique","de médenine":"tuniie","/arid regions institut":"tuniie","direction régionale":"dr","biologie structural":"biologietructurale",
     "délégation régionale":"dr","delegation regionale":"dr","inserm - délégation régionale provence alpes côte dazur et corse":"inermdrprovencealpecoteazurcorse",
     " dazur":"azur","(imim)":"","hospital universitario vall d'hebrón":"hopitaluniveritarivallhebron","faculty of medicine":"medicalfaculty",
     "german center for neurodegenerative diseases- munich":"germancenterneurodegenerativedieae","(dzne)":"",": inserm umr s_910":"umrs910",
     "génétique médical":"genetiquemedicale","_":"",": icn2 (csic & bist)":"","à":"a","(icn2)":"","mach: research and innovation centre":"mach",
     "â":"a","zuerich":"zurich","rotterdam - emc":"","ecole supérieure d'informatique: electronique etautomatique":"ecoleuperieuredinformatiqueelectroniqueautomatique",
     "féréérale":"federale"," lausane":"lausanne","ecole polytechnique federal":"ecolepolytechniquefederale","d’alger":"alger","d'ingenieurs":"dingenieur",
     "d'armement":"darmement","alger":"algerie","algiers alger":"algerie","(siem reap)":"",
     "department of computing: imperial college london":"departmentcomputingimperialcollege",
     "council for agriculture research and economics":"councilagriculturalreearcheconomics",
     "(idibaps)":"","(imm)":"","(cnr)":"","(csic)":"","z":"","pyrenees":"pyrenee","(cut)":"","de aragón":"",
     "de aragon":"","tecnloco":"tecnologico","del instituto politecnico nacional":"","/ université de brasilia":"","- cnes":"",
     "_bioénergétique et ingénierie des protéines":"bip","de sanaa":"","(crg)":"","dexperimentation":"experimentation",
     " (cete med )":""," - umifre n°16":"","detudes":"etude","physcis":"physics",
     "bilkent university - department of computer engineering":"bilkentuniverity","(beia)":""," - turquie":"","(ait)":"",
     "atominstitut techniche universität wien":"atomintituttechnicaluniverityvienna","areva stockage denergie":"arevatokagedenergie",
     "(apha)":"","alfred-wegener institute: helmholtz center for polar and marine science":"alfredwegenerintitute",
     "alfred wegener institute: helmholtz-zentrum für polar- und meeresforschung (awi)":"alfredwegenerintitute",
     "a2ia":"a2iaanalyeimageintelligenceartificielle","\xa0":"","ifremer - centre de nantes":"ifremer nantes",
     "humboldt-university:":"humboldtuniveritatzuberlin","humboldt university of berlin":"humboldtuniveritatzuberlin",
     "humboldt university berlin / institute of biology: experimental biophysics":"humboldtuniveritatzuberlin",
     "humboldt university berlin":"humboldtuniveritatzuberlin",
     "humboldt institute for internet and society":"humboldtuniveritatzuberlin","(hgugm)":"","(roumanie)":"",
     "hôpital européen g. pompidou: service of microbiology":"hopitaleuropeengeorgepompidou","hokkeido":"hokkaido",
     "helmholz zentrum münchen":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen: german research center for environmental health / research unit analytical biogeochemistry":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen german research center for environmental health":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen":"helmholtzzentrummunchenmunich","helmholtz zentrum muenchen gmbh":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen - german research center for environmental health (gmbh)":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen - german research center for environmental health (gmbh)":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen – deutsches forschungszentrum für gesundheit und umwelt gmbh (hmgu).":"helmholtzzentrummunchenmunich",
     "muenchen":"munchen"," gmbh ":"","ufz - allemagne":""," ufz ":""," gfz ":"","organization - demeter":"organiationdemeter",
     "(hao)":"","duesseldorf":"","heinrich-heine university dusseldorf":"heinrichheineuniveritat",
     "universität düsseldorf":"univeritatdueldorf","hebrew university hospital":"hebrewuniverity","medical faculty":"facultymedecine",
     "goethe-universität":"goetheuniveritatfrankfurt","enst bretagne":"ent","/ gesis":"","(idiv)":"","(dzne)":"","(dkfz)":"",
     "georg-august university - allemagne":"georgaugutuniveritygottingen","inserm umr s_910":"umrs910",
     "génétique médical":"genetiquemedicale"," inc.":"","(gmit)":"","invesztigacion":"investigacion",": icn2 (csic & bist)":"",
     "(icn2)":"","fundacio hospital universitari vall d’hebron (huvh) – institut de recerca (vhir)/ fundacio privada institut d’investigacio oncologica de vall d’hebron (vhio)":"fundaciohopitaluniveritarivallhebronintitutrecercafundacioprivadaintitutdinvetigaciooncologicavalldhebronvhio",
     "(hcb)":"","(fcrb)":"","biom?dica (fcrb) ? hospital clinic de barcelona":"","ce3c":"","t jena":"t","(fli)":"",
     "friedrich-alexander-universität":"friedrichalexanderuniverityerlangennuremberg","nümberg":"nuremberg",
     "french national scientific research center (cnrs)":"cnrs","universitaet":"universitat","(fsl)":"","'n'":"|n|",
     " i n i ":"|n|","fraunhofer ise":"fraunhoferintituteolarenergyystem","s ise":"","fraunhofer institute (fhg) -":""," e.v.":"",
     "foerderung":"forderung","(fist sa)":"","scientique":"scientifique","(fuel)":"",
     "foundation neurological institute c. besta":"fondazioneintitutoneurologicocarlobeta","(forth)":"",
     "foundation carlo besta neurological institute":"fondazioneintitutoneurologicocarlobeta","forshungszentrum":"forchungszentrum",
     "juelich":"julich","/cncs":"","irccs":"",": milan":""," carlo ":"c","istituto":"instituto","di milano - int":"","(indt)":"",
     "(int)":"","(sciences po)":""," sceinces":"sciences"," tse ":"","laffont toulouse sciences economiques":"laffont",
     "ujf-filiale":"filialeujf","flemish":"flemisch"," environmental institute":"institute of environment",
     " environment institute":"institute of environment","(fr)":"","marterials":"material"," the ":"",
     "univ porto (up)":"univerityporto","(cu)":""," resear ":"reearch","insitute":"institute"," hysical ":"physical",
     "vuinérables":"vulnerable","ées":"ee","- electricite de france":"","electricite de france -":"","electricité de france":"edf",
     "electricite de france":"edf","ville de paris":"pari","espci":"","gif-sur-yvette":"","electricite (sup":"electriciteupelec",
     "federal":"federale","(ens)":"","(oniris)":"","(ensv)":""," st ":"saint"," sant-":"saint","(ensm":"(ensma)","(ensa)":"",
     "(ean)":"","stras":"trabourg","clermont f":"clermontferrand","chauss":"chauee","(enpc)":"","(enac)":"",
     "traiement":"traitement","(ad2m)":"","(anses)":"","":"",
     "microtenhique":"microtechnique","microorgnismes":"microorganismes","università di cagliari":"universityofcagliari",
     "artctique":"arctique","besa":"beancon"," dele ":"delle","nazionalle":"nazionale",
     "alternaltive":"alternative","pyre":"pyrenee",".":"","scienctifique":"cientifique","agronomiqu":"agronomique",
     "besanco":"beancon","ème siècle":"eiecle","observatoie":"obervatoire","macromoléccules":"macromolecule","lyo":"lyon",
     "public et privé":"privepublic","structuale":"tructurale","wageningingen":"wageningen","minères":"miniere","(":"",")":"",
     "archéozzologie":"archeozoologie","alimentantion":"alimentation","sudorium":"tudorium",
     "ë":"e","ü":"u","i'":"l",":":""," te ":"","ò":"o"," i ":""," for ":"","ä":"a"," de ":"",
     "part":""," of ":""," en ":""," pour ":"","s":"","&":""," & ":""," et ":"",
     " and ":""," un ":""," une ":"",":":"","ó":"o"," à ":"a","í":"i",",":"",
     "ç":"c","û":"u","ê":"e","é":"e","è":"e",
     "à":"a","â":"a","ô":"o","î":"i"," de ":""," da":""," de":""," di":""," do":""," du":""," dh":"",
     " d'a":""," d'e":""," d'i":""," d'o":""," d'u":""," d'h":"",
     " d´a":""," d´e":""," d´i":""," d´o":""," d´u":""," d´h":"",
     " l'a":""," l'e":""," l'i":""," l'o":""," l'u":""," l'h":"",
     " l´a":""," l´e":""," l´i":""," l´o":""," l´u":""," l´h":"",
     " la":""," le":""," li":""," lo":""," lu":""," lh":"",
     "’":"","´":"","–":"","/":"",":":"","-":"","'":""," ":"","et":"","de":"","actalia food safety":""}

def replace_all(row):
    for i, j in dic.items():
        row = row.replace(i, j)
    return row


#fonction qui donne un idref à partir d'un ORCID
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
    
#fonction qui créé un dictionnaire pour une personne sous la forme: {"id":"idref", "first_name":"prénom", "last_name":"nom" }

def persons(row, prenom, nom):
    if pd.isna(row['id_structure'])==False and row['id_structure']!='x':
        dict_row={"id" : row['id_person'], "first_name": row[prenom], "last_name": row[nom], "role":f"scientific-officer###{str(row['id_structure'])}"}
    else:
        dict_row={"id" : row['id_person'], "first_name": row[prenom], "last_name": row[nom], "role":"scientific-officer"}
    dict_row2={k:v for k,v in dict_row.items() if (pd.isna(v)==False)}
    return dict_row2

#fonctions qui mets les titres et résumés sous forme de dictionnaire : {"fr": "titre ou résumé en français", "en": "titre ou résumé en anglais"}

def champs_dict(row,champ_fr,champ_en):
    if champ_en in list(row.keys()):
        if pd.isna(row[champ_fr])==False and pd.isna(row[champ_en])==False:
            return {"fr": row[champ_fr], "en": row[champ_en]}
        elif pd.isna(row[champ_fr]) and pd.isna(row[champ_en])==False:
            return {"en": row[champ_en]}
        elif pd.isna(row[champ_fr])==False and pd.isna(row[champ_en]):
            return {"fr": row[champ_fr]}
        else:
            return None
    if champ_en not in list(row.keys()) and champ_fr in list(row.keys()):
        if pd.isna(row[champ_fr])==False:
            return {"fr": row[champ_fr]}
        else:
            return None
    else:
        return None
#fonctions qui mets lalocalisation de la structure sous forme de dictionnaire : {"city": "ville où est basée la structure", "country": "pays où est basée la structure"}

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












    
