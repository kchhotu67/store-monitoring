from flask import Flask, request, jsonify, make_response, send_file
import random
import sqlite3
from datetime import datetime
import subprocess
import io

# create Flask instance
app = Flask(__name__)

def generate_report_id():
    timestamp = str(int(datetime.now().timestamp()))
    random_string = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=16)) 
    return timestamp + random_string


@app.route('/test', methods=['GET'])
def test_api():
    return jsonify({"status": "OK"})


@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    report_id = generate_report_id()
    if insert_report_id(report_id):
        subprocess.Popen(['python3', 'src/generate.py', report_id])
        return jsonify({"report_id": report_id})
    return jsonify({'message': 'Something went wrong!'}), 500


@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    report = get_report_by_id(report_id)
    if report is None:
        return jsonify({'message': 'Invalid report_id!'}), 404
    if report[0] == 'Complete':
        report_file = f'output/{report[1]}'
        with open(report_file, 'rb') as f:
            csv_content = f.read()
        # Send the file as a response
        return send_file(io.BytesIO(csv_content), mimetype='text/csv', download_name=report[1])

    else:
        return jsonify({'status': 'Running'})

def get_report_by_id(report_id):
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    select_query = "SELECT status, output_filename FROM store_report WHERE report_id = ?;"
    cursor.execute(select_query, (report_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def create_report_table():
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS store_report (
            report_id TEXT,
            status TEXT DEFAULT 'Running',
            output_filename TEXT DEFAULT '',
            PRIMARY KEY(report_id)
        );
    '''
    cursor.execute(create_table_query)
    conn.commit()

def insert_report_id(report_id):
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    query = "INSERT INTO store_report (report_id, status, output_filename) VALUES (?, 'Running', '');"
    try:
        cursor.execute(query, (report_id,))
        conn.commit()
        return True
    except Exception as e:
        print(e)
        return False

if __name__ == '__main__':

    # create store_report table if doesn't exits
    create_report_table()

    # start cron server for pulling and inserting latest data in mysql table
    subprocess.Popen(['python3', 'src/cron.py'])

    # start the flask server
    app.run(debug=False)

