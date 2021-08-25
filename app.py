import json
import os

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from pprint import pprint
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser
from prettytable import PrettyTable

app = Flask(__name__)

load_dotenv()

app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("SQLALCHEMY_DATABASE_URI")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class Alert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    strategy = db.Column(db.String(100))
    ticker = db.Column(db.String(20))
    interval = db.Column(db.Integer)
    action = db.Column(db.String(10))
    chartTime = db.Column(db.DateTime)
    time = db.Column(db.DateTime)
    chartPrice = db.Column(db.Numeric(20,10))
    price = db.Column(db.Numeric(20,10))    

with open('config.json', 'r') as f:
    config = json.load(f)
    
startBalance = config['startBalance']
fees = config['fees']
maxLeverage = config['maxLeverage']
riskStep = config['riskStep']
tickers = []
tickers = config['tickers']

@ app.route('/', methods=['GET'])
def main():    

    for ticker in tickers:        

        highestBalance = 0
        resultData = {}
        resultTable = PrettyTable()
        resultTable.field_names = ['Interval', 'Leverage', 'Risk', 'Final balance', 'No. of trades', 'No. of trades won', 'Highest profit %', 'No. of trades lost', 'Highest loss %', 'Trading hours']
        
        alerts = Alert.query.filter(Alert.ticker == ticker).order_by(Alert.id).all()        

        return {
            "ticker": ticker,
            "maxLeverage": maxLeverage,
            "startBalance": startBalance,
            "alerts": len(alerts)
        }
        
        '''
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

                while risk < 0.5:

                    risk += riskStep

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
                                    

                                # Open new position

                                buyBalance = currBalance * risk
                                feesAmount = buyBalance * leverage * fees
                                coinAmount = (buyBalance * leverage - feesAmount) / alertPrice
                                positionCost = buyBalance * leverage
                                lastBalance = currBalance
                                lastPrice = alertPrice

                                
                                print('Open Position', alertAction)
                                print('Alert Price:', alertPrice)
                                print('Current balance:', currBalance)
                                print('Coin amount:', coinAmount)
                                print('Fees amount:', feesAmount)
                                print('positionCost:', positionCost)
                                print()
                                

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

                        resultRow = []
                        resultRow.append(interval)
                        resultRow.append(leverage)
                        resultRow.append(risk)
                        resultRow.append(currBalance)
                        resultRow.append(noOfTrades)
                        resultRow.append(noOfTradesWon)
                        resultRow.append(highestProfit)
                        resultRow.append(noOfTradesLost)
                        resultRow.append(highestLoss)
                        resultRow.append(tradeHours)
                        resultTable.add_row(resultRow)

                
                print('Interval:', interval)
                print('Final balance:', currBalance)
                print('No. of trades:', noOfTrades)
                print('No. of trades won:', noOfTradesWon)
                print('Highest profit %:', highestProfit)
                print('No. of trades lost:', noOfTradesLost)
                print('Highest loss %:', highestLoss)
                print()
                
        
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
        

        resultTable.sortby = 'Final balance'
        resultTable.reversesort = True
        print(resultTable)
        '''