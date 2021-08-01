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
    fees = config['fees']
    leverage = config['leverage']

    if leverage == 0:
        leverage = 1

    print('Using leverage:', leverage)
    print('Start Balance:', startBalance)
    print()

    columnName = "VarCharValue"

    url = 'https://rt.pipedream.com/sql'
    hed = {'Authorization': 'Bearer ' + os.getenv("API_KEY")}

    data = {'query': "SELECT DISTINCT ticker FROM tradingview_alerts"}

    response = requests.post(url, json=data, headers=hed)

    resultSet = response.json()["resultSet"]
    rows = resultSet["Rows"]

    tickers = []
    rowCounter = 0
    for row in rows:
        rowCounter += 1
        columns = row["Data"]
        if rowCounter > 1:
            tickers.append(columns[0][columnName])

    for ticker in tickers:

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

        # intervals = [13]

        for interval in intervals:

            selectStr = "SELECT * FROM tradingview_alerts WHERE interval = '%s' ORDER BY time" % (str(interval))
            data = {'query': selectStr}

            response = requests.post(url, json=data, headers=hed)
            resultSet = response.json()["resultSet"]
            rows = resultSet["Rows"]

            rowCounter = 0

            lastAction = ""

            currBalance = startBalance
            lastBalance = 0
            noOfTrades = 0
            noOfTradesWon = 0
            noOfTradesLost = 0
            highestProfit = 0
            highestLoss = 0
            coinAmount = 0
            positionCost = 0

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

                        if coinAmount > 0:

                            # Close Position -> Not for first alert

                            feesAmount = coinAmount * alertPrice * fees
                            closeReturn = coinAmount * alertPrice - feesAmount

                            if alertAction == 'buy':
                                profit = positionCost - closeReturn
                            else:
                                profit = closeReturn - positionCost

                            currBalance = lastBalance + profit

                            profitPercent = (currBalance / lastBalance - 1) * 100

                            if profitPercent >= 0:
                                noOfTradesWon += 1
                            else:
                                noOfTradesLost += 1

                            if profitPercent > highestProfit:
                                highestProfit = profitPercent
                            if profitPercent < highestLoss:
                                highestLoss = profitPercent

                            '''
                            print('Close Position', alertAction)
                            print('Alert Price:', alertPrice)
                            print('Last balance:', lastBalance)
                            print('Close return:', closeReturn)
                            print('Position cost:', positionCost)
                            print('Coin amount:', coinAmount)
                            print('Fees amount:', feesAmount)
                            print('Current Balance:', currBalance)
                            print('Profit %', profitPercent)
                            print()
                            '''

                        # Open new position

                        feesAmount = currBalance * leverage * fees
                        coinAmount = (currBalance * leverage - feesAmount) / alertPrice
                        positionCost = currBalance * leverage
                        lastBalance = currBalance

                        '''
                        print('Open Position', alertAction)
                        print('Alert Price:', alertPrice)
                        print('Current balance:', currBalance)
                        print('Coin amount:', coinAmount)
                        print('Fees amount:', feesAmount)
                        print('positionCost:', positionCost)
                        print()
                        '''

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
