

# 6-5-2 의 전략


import pandas as pd
import time
import requests
import json
from datetime import datetime

import FinanceDataReader as fdr
from pykrx import stock as pystock

from dateutil.relativedelta import relativedelta
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

api_key = config['hantu']['api_key']
secret_key = config['hantu']['secret_key']
account_id = config['hantu']['account_id']


# HantuStock 패키지 불러오기
from HantuStock import HantuStock

ht = HantuStock(api_key=api_key,secret_key=secret_key,account_id=account_id)

# 전략 관련 데이터를 저장할 json 파일 생성
try:
    with open('strategy_data.json','r') as f:
        strategy_data = json.load(f)
except:
    # strategy_data.json 파일이 존재하지 않는 경우
    strategy_data = {
        'holding_period': {} # 종목별 보유기간
        }
    

# 현재 보유중인 종목 조회

holdings = ht.get_holding_stock()

# holding_period를 하루씩 높여줌
# holding_period가 3 이상인 종목은 종가에 매도(매수 3일차 종가에 매도)
for tkr in holdings:
    if tkr not in strategy_data['holding_period']:
        # 처음엔 holding_period에 티커값이 저장되어있지 않음. 따라서, 1로 초기화
        strategy_data['holding_period'][tkr] = 1
    else:
        # 값이 저장되어있는 종목은 1씩 값을 높여줌
        strategy_data['holding_period'][tkr] += 1

# 보유 종목 중 당일 종가에 매도할 종목 정리
ticker_to_sell = []
for tkr in holdings:
    if strategy_data['holding_period'][tkr] >= 3:
        ticker_to_sell.append(tkr)


# 전략의 시간을 체크할 while문 작성
while True:
    current_time = datetime.now()

    # 여기 내부에서 전략이 실행됨 

    if current_time.hour == 15 and current_time.minute == 20:
        # 종가 매도할 종목 매도주문
        for tkr in ticker_to_sell:
            ht.ask(tkr,'market',holdings[tkr],'STOCK')
            strategy_data['holding_period'][tkr] = 0 # 매도후엔 holding_period를 초기화해줌


        # 종가 진입종목 탐색
        data = ht.get_past_data_total(n=20)
        
        # 5일 종가 최저값, 20일 이동평균 계산하기
        data['5d_min_close'] = data.groupby('ticker')['close'].rolling(5).min().reset_index().set_index('level_1')['close']
        data['20d_ma'] = data.groupby('ticker')['close'].rolling(20).mean().reset_index().set_index('level_1')['close']

        # 조건에 맞는 종목 찾기 - 최근 5일 종가 중 오늘 종가가 가장 낮고, 20일 이동평균보다 종가가 더 낮은 경우
        today_data = data[data['timestamp'] == data['timestamp'].max()]
        today_data = today_data[(today_data['5d_min_close'] == today_data['close']) & (today_data['20d_ma'] > today_data['close'])]

        # 지금 보유중인 종목은 매수후보에서 제외
        today_data = today_data[~today_data['ticker'].isin(holdings.keys())]

        # 그 중 거래량이 가장 많았던 10종목 고르기
        entry_tickers = list(today_data.sort_values('trade_amount')[-10:]['ticker'])

        # 선정한 종목 매수
        for tkr in entry_tickers:
            ht.bid(tkr,'market',1,'STOCK')

        break

    # 루프 돌때마다 1초씩 쉬어줌
    time.sleep(1)