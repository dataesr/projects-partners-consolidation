import numpy as np
import pandas as pd
 
#get the names of scanR structures
def get_scanR_structure(x):
    if (isinstance(x, dict)):
        if pd.isna(x['label'].get('default'))!=True:
            return x['label'].get('default').split('__-__')[0]
        else:
            return None
    else:
        return None

#get the best id   
def get_id(row,colonnes):
    for col in colonnes:
        if row[col]!=None and str(row[col])!='nan' and str(row[col])!='None' and str(row[col])!='NaN' and row[col] is not np.nan and str(row[col])[2:10]!='homonyms' and str(row[col])[3:11]!='homonyms':
            return row[col]
    return None
        
#obtain a simple list from a list of lists
def flatten(conteneurs):
    return [conteneurs[i][j] for i in range(len(conteneurs)) for j in range(len(conteneurs[i]))]
  
#dictionnary of corrected typing errors:
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
    

#get the first names from ADEME data
def extract_first_name(x):
    parts = str(x).split(' ')
    if len(parts) > 1:  
        part2 = parts[1].split('-')
        prenom = part2[0].capitalize()  
        if len(part2) > 1:
            prenom += '-' + '-'.join([p.capitalize() for p in part2[1:]])
        return prenom
    else:
        return ''
    
#get the string without '"'
def strip_outer_quotes(val):
    if isinstance(val, str) and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val

#get the budget cleaned
def clean_budget(x):
    x_str = str(x).replace(' ', '').replace('€', '').replace('\x80', '').replace(',', '.')
    if x_str.find('.')!=-1 and x_str.split('.')[-1]!='0' and x_str.split('.')[-1]!='00':
        return float(x_str)
    elif x_str=='nan':
        return None
    else:
        return int(x_str.replace('.0','').replace('.00',''))







    
