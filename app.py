import os  # Import os to read environment variables
import psycopg2
from flask import Flask, jsonify
import pandas as pd

# Connect to Supabase (PostgreSQL) using environment variables
try:
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME")
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
    return jsonify({"max_drawdown": "-15%"})

@app.route('/get_data')
def get_data():
    try:
        cur = conn.cursor()
        cur.execute('SELECT * FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10;')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}, 500

@app.route('/average_close')
def average_close():
    try:
        cur = conn.cursor()
        cur.execute('SELECT close FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10000000;')
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['close'])
        average = df['close'].mean()
        return {"average_close": average}
    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")


