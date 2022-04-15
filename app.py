import json
import os
import pandas as pd

from flask import Flask, jsonify, request, render_template
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
    
pd.options.display.float_format = '{:,.2f}'.format

@ app.route('/', methods=['GET'])
def main():    
    
    leverage = int(request.args.get('leverage'))
    risk = float(request.args.get('risk'))
    startBalance = int(request.args.get('startBalance'))
    fees = float(request.args.get('fees'))    
    
    df = pd.DataFrame()
    
    strategies = []
    strategyRows = db.session.query(Alert.strategy).distinct().all()
    for strategyRow in strategyRows:
        strategy = strategyRow["strategy"]
        strategies.append(strategy)    
        
    for strategy in strategies:
    
        tickers = []
        
        tickerRows = db.session.query(Alert.ticker).filter(Alert.strategy == strategy).distinct().all()
        for tickerRow in tickerRows:
            ticker = tickerRow["ticker"]
            tickers.append(ticker)        
        
        for ticker in tickers:      
                        
            intervals = []  
            
            intervalRows = db.session.query(Alert.interval).filter(Alert.strategy == strategy, Alert.ticker == ticker).distinct().all()
            for intervalRow in intervalRows:                
                interval = intervalRow["interval"]
                intervals.append(interval)                
                
            for interval in intervals:                
                
                alerts = Alert.query.filter(Alert.strategy == strategy, Alert.ticker == ticker, Alert.interval == interval).order_by(Alert.id).all()
                    
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
                            
                        # Open new position

                        buyBalance = currBalance * risk
                        feesAmount = buyBalance * leverage * fees
                        coinAmount = (buyBalance * leverage - feesAmount) / alertPrice
                        positionCost = buyBalance * leverage
                        lastBalance = currBalance
                        lastPrice = alertPrice

                    lastAction = alertAction

                profitPercent = (currBalance / startBalance - 1) * 100
                if noOfTrades > 0:
                    winRate = noOfTradesWon / noOfTrades
                else:
                    winRate = 0
                endDateTime = alertTime
                timeDiff = endDateTime - startDateTime
                tradeHours = round(timeDiff.total_seconds() / 3600, 0)                    
                
                resultData = {}
                resultData["strategy"] = strategy
                resultData["ticker"] = ticker                    
                resultData["interval"] = interval
                resultData["leverage"] = leverage
                resultData["risk"] = risk
                resultData["endBalance"] = currBalance
                resultData["profit"] = profitPercent
                resultData["noOfTrades"] = noOfTrades
                resultData["noOfTradesWon"] = noOfTradesWon
                resultData["noOfTradesLost"] = noOfTradesLost
                resultData["winRate"] = winRate
                resultData["tradeHours"] = tradeHours
                
                df = df.append(resultData, ignore_index=True)                        
    
    df.sort_values(['strategy','ticker','interval'], inplace=True, ascending=True)
    df['interval'] = df['interval'].map('{:,.0f}'.format)
    df['leverage'] = df['leverage'].map('{:,.0f}'.format)
    df['noOfTrades'] = df['noOfTrades'].map('{:,.0f}'.format)
    df['noOfTradesWon'] = df['noOfTradesWon'].map('{:,.0f}'.format)
    df['noOfTradesLost'] = df['noOfTradesLost'].map('{:,.0f}'.format)
    df['tradeHours'] = df['tradeHours'].map('{:,.0f}'.format)
    
    df.rename(columns={ 'strategy': 'Strategy',
                        'ticker': 'Ticker',
                        'interval': 'Interval min',
                        'leverage': 'Leverage',
                        'risk': 'Risk',
                        'endBalance': 'End balance',
                        'profit': 'Profit %',
                        'noOfTrades': 'No. of trades',
                        'noOfTradesWon': 'No. of trades won',
                        'noOfTradesLost': 'No. of trades lost',
                        'winRate': 'Win rate',
                        'tradeHours': "Trading hours"
                            }, inplace=True)

    return render_template('data.html',  tables=[df.to_html(classes='data', header="true")])

        