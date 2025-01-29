# Repository for Processing French Funding Data

In this repository, we process French funding data by adding information about partner organizations and affiliated persons.

## Setup Instructions

To use it, first, create a `.env` file in the root directory and add authorization access. Then, there are three options:

### 1. Run the notebook `process_and_send_data_details.ipynb`

Run the notebook with all the details of the code, step by step. When using the Jupyter notebook, select the data source (ex: ANR, ANSES...) at the beginning, and then execute each cell to obtain a standardized format for one source at a time.

### 2. Run the Flask application locally

- In an IDE, execute: `python projects_partners_flask_app.py`
- Then, execute the process at `http://127.0.0.1:5000/process?source=` + the data source (ex: ANR, ANSES...).
- When the process is over, you can update the new projects or partners with:
  ### 1. update the projects with the link:
  - `http://127.0.0.1:5000/update/project?source=` + the data source (ex: ANR, ANSES...)
    - example: `curl -X POST "http://127.0.0.1:5000/update/project?source=ANSES"`
  ### 2. update the partners
  - `http://127.0.0.1:5000/update/partner?source=` + the data source (ex: ANR, ANSES...)
    - example: `curl -X POST "http://127.0.0.1:5000/update/partner?source=ANSES"`

### 3. Run the Flask application remotely

- blabla
