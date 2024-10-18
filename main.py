from flask import Flask, render_template, request
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

main = Flask(__name__)

# Database connection configuration
connection_string = os.getenv('connection_string')
conn = pyodbc.connect(connection_string)

@main.route('/')
def home():
    return render_template('index.html')

@main.route('/insert', methods=['POST'])
def insert_data():
    num_queries = int(request.form['num_queries'])
    
    cursor = conn.cursor()
    
    for _ in range(num_queries):
        # Generate random values for the data
        # Replace this with your own logic to generate random tuples
        
        # Example: Generating random values
        time = '2023-06-19 12:00:00'
        latitude = 1.234
        longitude = 5.678
        depth = 10.5
        mag = 6.7
        net = 'NET'
        record_id = 'ID'
        updated = '2023-06-19 12:00:00'
        place = 'Some Place'
        record_type = 'Type'
        
        # Insert the data into the table
        cursor.execute("INSERT INTO EQ (time, latitude, longitude, depth, mag, net, id, updated, place, type) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       time, latitude, longitude, depth, mag, net, record_id, updated, place, record_type)
    
    conn.commit()
    
    return 'Data inserted successfully'

if __name__ == '__main__':
    main.run()
