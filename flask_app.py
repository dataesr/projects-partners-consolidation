import pandas as pd
from flask import Flask, request, jsonify, render_template

from code_utils.formatting_data_projects import formatting_projects_data
from code_utils.process_data import cache, get_data, get_id_structure, get_id_person
from code_utils.send_or_update_data import send_data, send_only_newer_data

app=Flask(__name__)

sources=pd.read_json('sources.json')

@app.route('/', methods=['GET'])
def select_source():
    return render_template('select_source.html')

@app.route('/process', methods=['GET'])
def process_data():
    args = request.args
    source = args.get('source')
    cached_data, cached_data_persons, cached_data_orcid = cache(source)
    df_partenaires = get_data(sources,source)
    df_partenaires_structures = get_id_structure(df_partenaires, source, sources, cached_data)
    get_id_person(df_partenaires_structures, source, sources, cached_data_persons, cached_data_orcid)
    return {}, 202

@app.route('/send_projects', method=['POST']) 
def send_projects():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_data(df_projects, 'http://185.161.45.213/projects/projects')

@app.route('/update_projects', method=['POST']) 
def update_projects():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_only_newer_data(df_projects, 'http://185.161.45.213/projects/projects')

@app.route('/send_partners', method=['POST']) 
def send_partners():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_data(df_projects, 'http://185.161.45.213/projects/projects')

@app.route('/update_partners', method=['POST']) 
def update_partners():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_only_newer_data(df_projects, 'http://185.161.45.213/projects/projects')

if __name__ == '__main__':
    app.run(debug=True)