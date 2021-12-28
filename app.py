import json
import os
import pandas as pd

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
    
    results = []
    
    if not tickers:
        tickerRows = db.session.query(Alert.ticker).distinct().all()
        for tickerRow in tickerRows:
            ticker = tickerRow["ticker"]
            tickers.append(ticker)   

    for ticker in tickers:
        strategies = []
        strategyRows = db.session.query(Alert.strategy).distinct().all()
        for strategyRow in strategyRows:
            strategy = strategyRow["strategy"]
            strategies.append(strategy)
            
        for strategy in strategies:
            highestBalance = 0            
            df = pd.DataFrame()         
            
            intervals = []  
            intervalRows = db.session.query(Alert.interval).distinct().all()
            for intervalRow in intervalRows:                
                interval = intervalRow["interval"]
                intervals.append(interval)
                
                for interval in intervals:                    
                    
                    alerts = Alert.query.filter(Alert.ticker == ticker, Alert.interval == interval, Alert.strategy == strategy).order_by(Alert.id).all()

                    leverage = 0
                    
                    while leverage < maxLeverage:
                        
                        leverage += 1

                        risk = 0
                        
                        # while risk < 0.25:
                            
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
                                    
                                # Open new position

                                buyBalance = currBalance * risk
                                feesAmount = buyBalance * leverage * fees
                                coinAmount = (buyBalance * leverage - feesAmount) / alertPrice
                                positionCost = buyBalance * leverage
                                lastBalance = currBalance
                                lastPrice = alertPrice

                            lastAction = alertAction

                        endDateTime = alertTime
                        timeDiff = endDateTime - startDateTime
                        tradeHours = round(timeDiff.total_seconds() / 3600, 0)

                        # TODO: Why are there so less results ?? -> Debug

                        if not liquidated:
                            if currBalance > highestBalance:
                                highestBalance = currBalance
                                
                                resultData = {}
                                resultData["ticker"] = ticker
                                resultData["strategy"] = strategy
                                resultData["interval"] = interval
                                resultData["maxLeverage"] = maxLeverage
                                resultData["leverage"] = leverage
                                resultData["risk"] = risk
                                resultData["highestBalance"] = highestBalance
                                resultData["noOfTrades"] = noOfTrades
                                resultData["noOfTradesWon"] = noOfTradesWon
                                resultData["highestProfit"] = highestProfit
                                resultData["noOfTradesLost"] = noOfTradesLost
                                resultData["highestLoss"] = highestLoss
                                resultData["tradeHours"] = tradeHours
                                
                                df = df.append(resultData, ignore_index=True)                                         
                        
            print(strategy)
            print(ticker)
            print('Number of records', len(df))
            df.sort_values(by=['highestBalance'], ascending=False)            
            row = df.head(1).to_json(orient='records')
            result = json.loads(row)
            
            results.append(result)
            
            
    return {            
        "Results": results
    }
        