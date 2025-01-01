from flask import Flask, jsonify
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

@app.route('/get_data')
def get_data():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Failed to connect to database"}), 500

    try:
        cur = conn.cursor()
        cur.execute('SELECT * FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10;')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

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


