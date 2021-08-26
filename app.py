import json
import os

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from pprint import pprint
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser

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
        
        # resultTable.field_names = ['Interval', 'Leverage', 'Risk', 'Final balance', 'No. of trades', 'No. of trades won', 'Highest profit %', 'No. of trades lost', 'Highest loss %', 'Trading hours']
        
        # TODO Get DB records into JSON with Marshmellow        
                
        intervalRows = db.session.query(Alert.interval).distinct().all()
        
        for intervalRow in intervalRows:     
            
            interval = intervalRow["interval"]
            
            alerts = Alert.query.filter(Alert.ticker == ticker, Alert.interval == interval).order_by(Alert.id).all()

            leverage = maxLeverage

            risk = riskStep

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

            for alert in alerts:
                rowCounter += 1
                
                alertTime = alert.time
                alertTicker = alert.ticker
                alertInterval = alert.interval
                alertAction = alert.action
                alertPrice = float(alert.price)

                if rowCounter == 1:
                    startDateTime = alertTime

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

                endDateTime = alertTime
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
        
        
        return {            
            'Interval': resultData["interval"],
            'Leverage': resultData["leverage"],
            'Risk': resultData["risk"],
            'Final balance': resultData["highestBalance"],
            'No. of trades': resultData["noOfTrades"],
            'No. of trades won': resultData["noOfTradesWon"],
            'Highest profit %': resultData["highestProfit"],
            'No. of trades lost': resultData["noOfTradesLost"],
            'Highest loss %': resultData["highestLoss"],
            'Trading hours': resultData["tradeHours"]
        }
        