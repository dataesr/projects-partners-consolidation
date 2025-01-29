import pandas as pd
from flask import Flask, request, jsonify, render_template

from project.server.main.formatting_data_partners import formatting_partners_data
from project.server.main.formatting_data_projects import formatting_projects_data
from project.server.main.logger import get_logger
from project.server.main.process_data import cache, get_data, get_id_structure, get_id_person
from project.server.main.send_or_update_data import send_data, send_only_newer_data

app=Flask(__name__)
logger = get_logger(__name__)
sources=pd.read_json('sources.json')

@app.route('/', methods=['GET'])
def home():
    return "Bonjour"

@app.route('/process', methods=['GET'])
def process_data():
    try:
        args = request.args
        source = args.get('source')
        if not source:
            logger.error("the'source' parameter is missing")
            return jsonify({"error": "Missing 'source' parameter"}), 400
        logger.debug(f"starting data processing for source: {source}")
        cached_data, cached_data_persons, cached_data_orcid = cache(source)
        df_partenaires = get_data(sources,source)
        df_partenaires_structures = get_id_structure(df_partenaires, source, sources, cached_data)
        get_id_person(df_partenaires_structures, source, sources, cached_data_persons, cached_data_orcid)
        logger.debug(f"processing completed for source: {source}")
        return jsonify({"message": "Processing completed successfully", "source": source}), 202
    except Exception as e:
        logger.error(f"error during data processing: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/send/project', methods=['GET','POST']) 
def send_projects():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_data(df_projects, 'http://185.161.45.213/projects/projects')
    
@app.route('/send/partner', methods=['GET','POST']) 
def send_partners():
    args = request.args
    source = args.get('source')
    df_projects=formatting_partners_data(sources, source)
    send_data(df_projects, 'http://185.161.45.213/projects/participations')

@app.route('/update/project', methods=['GET','POST']) 
def update_projects():
    args = request.args
    source = args.get('source')
    df_projects=formatting_projects_data(sources, source)
    send_only_newer_data(df_projects, 'http://185.161.45.213/projects/projects', 'projects', source)
    return jsonify({"message": "Update completed successfully", "source": source}), 202

@app.route('/update/partner', methods=['GET','POST']) 
def update_partners():
    args = request.args
    source = args.get('source')
    df_projects=formatting_partners_data(sources, source)
    send_only_newer_data(df_projects, 'http://185.161.45.213/projects/participations', 'partners', source)
    return jsonify({"message": "Update completed successfully", "source": source}), 202

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')