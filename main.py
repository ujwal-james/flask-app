from flask import Flask, jsonify, request,render_template
import pandas as pd
import sqlite3


app = Flask(__name__)

@app.route("/")
def home():
     return render_template('index.html',title="Welcome")


@app.route("/create")
def create():
    data = pd.read_excel("production_data.xls")
    annual_data = data.groupby("API WELL NUMBER").sum()
    dbname = 'production'
    connection = sqlite3.connect(dbname + '.sqlite')
    cursor = connection.cursor()


    # Create the table
    cursor.execute("CREATE TABLE IF NOT EXISTS production (well_number TEXT, oil INT, gas INT, brine INT)")

    # Insert the data into the table
    for _, row in annual_data.iterrows():
        # print(row["OIL"])
        cursor.execute("INSERT INTO production VALUES (?, ?, ?, ?)",
                    (row.name, row["OIL"], row["GAS"], row["BRINE"]))

    # Commit the changes and close the connection
    connection.commit()
    connection.close()
    return render_template('index.html',title="Db Created")
    

@app.route("/data", methods=["GET"])
def get_data():
    well_number = request.args.get("well")
    # well_number='34059242540000'

    connection = sqlite3.connect("production.sqlite")
    cursor = connection.cursor()

    # Retrieve the data for the requested well
    cursor.execute("SELECT oil, gas, brine FROM production WHERE well_number = ?", (well_number,))
    result = cursor.fetchone()
    connection.close()

    if result is None:
        return jsonify({"error": "Well not found"}), 404

    oil, gas, brine = result
    return jsonify({"oil": oil, "gas": gas, "brine": brine})

if __name__ == "__main__":
    app.run(port=8080)