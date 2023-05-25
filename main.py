from flask import Flask, jsonify, request
import pandas as pd
import sqlite3

app = Flask(__name__)

@app.route("/")
def home():
     return "Welcome <br> <a href=./create>create db</a> <br> <a href=http://localhost:8080/data?well=34059242540000>test sample for http://localhost:8080/data?well=34059242540000</a>  <br> <br><form action=./data?well=data> <input type=text name=well placeholder=data required /> <br><br> <input type=submit value=submit></form>"


@app.route("/create")
def create():
    data = pd.read_excel("production_data.xls")
    annual_data = data.groupby("API WELL NUMBER").sum()
    dbname = 'production'
    connection = sqlite3.connect(dbname + '.sqlite')
    cursor = connection.cursor()

    
    cursor.execute("CREATE TABLE IF NOT EXISTS production (well_number TEXT, oil INT, gas INT, brine INT)")

    for _, row in annual_data.iterrows():
       
        cursor.execute("INSERT INTO production VALUES (?, ?, ?, ?)",
                    (row.name, row["OIL"], row["GAS"], row["BRINE"]))

    connection.commit()
    connection.close()
    return "db created   <br> <a href=http://localhost:8080/data?well=34059242540000>test sample for http://localhost:8080/data?well=34059242540000</a>  <br> <br><form action=./data?well=data> <input type=text name=well placeholder=data required /> <br><br> <input type=submit value=submit></form>"
    

@app.route("/data", methods=["GET"])
def get_data():
    well_number = request.args.get("well")

    connection = sqlite3.connect("production.sqlite")
    cursor = connection.cursor()

    cursor.execute("SELECT oil, gas, brine FROM production WHERE well_number = ?", (well_number,))
    result = cursor.fetchone()
    connection.close()

    if result is None:
        return jsonify({"error": "Well not found"}), 404

    oil, gas, brine = result
    return jsonify({"oil": oil, "gas": gas, "brine": brine})

if __name__ == "__main__":
    app.run(port=8080)
