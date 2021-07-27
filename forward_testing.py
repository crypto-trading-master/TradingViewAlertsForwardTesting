from pprint import pprint
from dotenv import load_dotenv
import json
import requests
import os


def run():

    with open('config.json', 'r') as f:
        config = json.load(f)

    load_dotenv()

    startBalance = config['startBalance']
    fees = ['fees']
    leverage = config['leverage']

    if leverage == 0:
        leverage = 1

    print('Using leverage:', leverage)
    print('Start Balance:', startBalance)
    print()

    columnName = "VarCharValue"

    url = 'https://rt.pipedream.com/sql'
    hed = {'Authorization': 'Bearer ' + os.getenv("API_KEY")}
    data = {'query': "SELECT DISTINCT interval FROM tradingview_alerts"}

    response = requests.post(url, json=data, headers=hed)

    resultSet = response.json()["resultSet"]
    rows = resultSet["Rows"]

    intervals = []
    rowCounter = 0
    for row in rows:
        rowCounter += 1
        columns = row["Data"]
        if rowCounter > 1:
            intervals.append(int(columns[0][columnName]))

    intervals.sort()

    for interval in intervals:

        selectStr = "SELECT * FROM tradingview_alerts WHERE interval = '%s' ORDER BY time" % (str(interval))
        data = {'query': selectStr}

        response = requests.post(url, json=data, headers=hed)
        resultSet = response.json()["resultSet"]
        rows = resultSet["Rows"]

        rowCounter = 0

        lastPrice = 0
        lastAction = ""

        currBalance = startBalance
        noOfTrades = 0
        noOfTradesWon = 0
        noOfTradesLost = 0
        highestProfit = 0
        highestLoss = 0

        for row in rows:
            rowCounter += 1
            columns = row["Data"]
            if rowCounter > 1:
                alertTime = columns[0][columnName]
                alertTicker = columns[1][columnName]
                alertInterval = columns[2][columnName]
                alertAction = columns[3][columnName]
                alertPrice = float(columns[4][columnName])

                if alertAction != lastAction:

                    noOfTrades += 1

                    if lastPrice == 0:
                        lastPrice = alertPrice
                    if alertAction == 'buy':
                        profit = ((lastPrice / alertPrice) - 1) * leverage
                    else:
                        profit = ((alertPrice / lastPrice) - 1) * leverage

                    if profit >= 0:
                        noOfTradesWon += 1

                    else:
                        noOfTradesLost += 1

                    profitPercent = profit * 100
                    if profitPercent > highestProfit:
                        highestProfit = profitPercent
                    if profitPercent < highestLoss:
                        highestLoss = profitPercent

                    currBalance *= (1 + profit)

                    '''
                    print('Action:', alertAction)
                    print('Last price:', lastPrice)
                    print('Alert price:', alertPrice)
                    print('Profit:', profit)
                    print('Profit %:', profitPercent)
                    print('Current balance:', currBalance)
                    print()
                    '''

                    lastPrice = alertPrice

                lastAction = alertAction

        print('Interval:', interval)
        print('Final balance:', currBalance)
        print('No. of trades:', noOfTrades)
        print('No. of trades won:', noOfTradesWon)
        print('Highest profit %:', highestProfit)
        print('No. of trades lost:', noOfTradesLost)
        print('Highest loss %:', highestLoss)
        print()


if __name__ == "__main__":
    run()
