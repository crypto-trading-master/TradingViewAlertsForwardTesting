from pprint import pprint
from dotenv import load_dotenv
import json
import requests
import os


def run():

    with open('config.json', 'r') as f:
        config = json.load(f)

    load_dotenv()

    url = 'https://rt.pipedream.com/sql'
    hed = {'Authorization': 'Bearer ' + os.getenv("API_KEY")}
    data = {'query': "SELECT * FROM tradingview_alerts ORDER BY time"}

    response = requests.post(url, json=data, headers=hed)

    resultSet = response.json()["resultSet"]
    rows = resultSet["Rows"]

    rowCounter = 0
    column_caption = []

    for row in rows:
        rowCounter += 1
        columns = row["Data"]
        if rowCounter > 1:
            alert_time = columns[0]["VarCharValue"]
            alert_ticker = columns[1]["VarCharValue"]
            alert_interval = columns[2]["VarCharValue"]
            alert_action = columns[3]["VarCharValue"]
            alert_price = columns[4]["VarCharValue"]

            print(alert_time)
            print(alert_ticker)
            print(alert_interval)
            print(alert_action)
            print(alert_price)


if __name__ == "__main__":
    run()
