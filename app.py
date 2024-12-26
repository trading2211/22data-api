from flask import Flask, jsonify
import psycopg2
import pandas as pd
import os

# Connect to Supabase (PostgreSQL)
try:
    conn = psycopg2.connect(
        user="postgres.ahfglshexgeplzdlkrlf",
        password="JeVisEnEurope",
        host="aws-0-eu-west-3.pooler.supabase.com",
        port="5432",  # Default PostgreSQL port
        dbname="postgres"
    )
    print("Successfully connected to Supabase!")
except Exception as e:
    print("Error connecting to Supabase:", e)
    conn = None

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return "Bienvenue dans ma maison magique !"

@app.route('/max_drawdown')
def max_drawdown():
    # Example placeholder calculation
    return jsonify({"max_drawdown": "-15%"})

@app.route('/get_data')
def get_data():
    try:
        cur = conn.cursor()
        # Query data sorted by date to ensure consistency
        cur.execute('SELECT * FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10;')
        rows = cur.fetchall()

        # Fetch column names for better readability
        columns = [desc[0] for desc in cur.description]
        
        # Convert rows into a list of dictionaries
        data = [dict(zip(columns, row)) for row in rows]
        
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}, 500



@app.route('/average_close')
def average_close():
    try:
        cur = conn.cursor()
        # Query the 'close' column with consistent ordering
        cur.execute('SELECT close FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10000000;')
        rows = cur.fetchall()

        # Convert data into a Pandas DataFrame
        df = pd.DataFrame(rows, columns=['close'])
        
        # Calculate the average close price
        average = df['close'].mean()
        
        return {"average_close": average}
    except Exception as e:
        return {"error": str(e)}, 500


if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")

