import json
import requests
import os
from pprint import pprint
from dotenv import load_dotenv
from dateutil import parser


def run():

    with open('config.json', 'r') as f:
        config = json.load(f)

    load_dotenv()

    tableName = config['tableName']
    startBalance = config['startBalance']
    fees = config['fees']
    maxLeverage = config['maxLeverage']
    risk = config['risk']

    print('Max. leverage:', maxLeverage)
    print('Start Balance:', startBalance)
    print()

    columnName = "VarCharValue"

    url = 'https://rt.pipedream.com/sql'
    hed = {'Authorization': 'Bearer ' + os.getenv("API_KEY")}

    selectStr = "SELECT DISTINCT ticker FROM %s" % (tableName)
    data = {'query': selectStr}

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

        print('Ticker:', ticker)
        print()

        highestBalance = 0
        resultData = {}

        selectStr = "SELECT DISTINCT interval FROM %s WHERE ticker = '%s'" % (tableName, ticker)

        data = {'query': selectStr}

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

            print('Calculate interval', interval)

            selectStr = "SELECT * FROM %s WHERE interval = '%s' AND ticker = '%s' ORDER BY time" % (tableName, str(interval), ticker)

            data = {'query': selectStr}

            response = requests.post(url, json=data, headers=hed)
            resultSet = response.json()["resultSet"]
            rows = resultSet["Rows"]

            leverage = 0

            while leverage < maxLeverage:

                leverage += 1

                risk = 0

                while risk < 1:

                    risk += 0.01

                    rowCounter = 0
                    lastAction = ""

                    liquidated = False
                    currBalance = startBalance
                    lastBalance = 0
                    lastPrice = 0
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

                            if rowCounter == 2:
                                startDateTime = parser.parse(alertTime)

                            if alertAction != lastAction:

                                if coinAmount > 0:

                                    noOfTrades += 1

                                    # Close Position -> Not for first alert

                                    feesAmount = coinAmount * alertPrice * fees
                                    closeReturn = coinAmount * alertPrice - feesAmount

                                    if alertAction == 'buy':
                                        profit = positionCost - closeReturn
                                    else:
                                        profit = closeReturn - positionCost

                                    currBalance = lastBalance + profit

                                    profitPercent = (currBalance / lastBalance - 1) * leverage * 100

                                    if profitPercent >= 0:
                                        noOfTradesWon += 1
                                    else:
                                        noOfTradesLost += 1

                                    if profitPercent > highestProfit:
                                        highestProfit = profitPercent
                                    if profitPercent < highestLoss:
                                        highestLoss = profitPercent

                                    if profitPercent <= -100:
                                        liquidated = True

                                    '''
                                    print('Close Position', alertAction)
                                    print('Alert Price:', alertPrice)
                                    print('Last Price:', lastPrice)
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

                                buyBalance = currBalance * risk
                                feesAmount = buyBalance * leverage * fees
                                coinAmount = (buyBalance * leverage - feesAmount) / alertPrice
                                positionCost = buyBalance * leverage
                                lastBalance = currBalance
                                lastPrice = alertPrice

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

                            endDateTime = parser.parse(alertTime)
                            timeDiff = endDateTime - startDateTime
                            tradeHours = round(timeDiff.total_seconds() / 3600, 0)

                    if not liquidated:
                        if currBalance > highestBalance:
                            highestBalance = currBalance

                            resultData["interval"] = interval
                            resultData["leverage"] = leverage
                            resultData["risk"] = risk
                            resultData["highestBalance"] = highestBalance
                            resultData["noOfTrades"] = noOfTrades
                            resultData["noOfTradesWon"] = noOfTradesWon
                            resultData["highestProfit"] = highestProfit
                            resultData["noOfTradesLost"] = noOfTradesLost
                            resultData["highestLoss"] = highestLoss
                            resultData["tradeHours"] = tradeHours

                '''
                print('Interval:', interval)
                print('Final balance:', currBalance)
                print('No. of trades:', noOfTrades)
                print('No. of trades won:', noOfTradesWon)
                print('Highest profit %:', highestProfit)
                print('No. of trades lost:', noOfTradesLost)
                print('Highest loss %:', highestLoss)
                print()
                '''

        print()
        print('Best result:')
        print('Interval:', resultData["interval"])
        print('Leverage:', resultData["leverage"])
        print('Risk:', resultData["risk"])
        print('Final balance:', resultData["highestBalance"])
        print('No. of trades:', resultData["noOfTrades"])
        print('No. of trades won:', resultData["noOfTradesWon"])
        print('Highest profit %:', resultData["highestProfit"])
        print('No. of trades lost:', resultData["noOfTradesLost"])
        print('Highest loss %:', resultData["highestLoss"])
        print('Trading hours', resultData["tradeHours"])
        print()


if __name__ == "__main__":
    run()
