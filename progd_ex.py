from os import listdir
from os.path import isfile, join, getsize
from operator import itemgetter
import pandas as pd
import datetime
import numpy as np
import matplotlib.pyplot as plt

total_money = 1  # starting with 1$
both_money = 1
purchased = {}  # quantity,buy price,buy date,sell date
min_date = '1970-01-01'
current_date = '1960-01-01'
keep_time = 120  # max time before sell,3 months !!! must be equal when searching
file_names = [f for f in listdir('C:/Users/tzagk/Downloads/Stocks') if
              isfile(join('C:/Users/tzagk/Downloads/Stocks', f))]
dates_dict = {}  # keeping all csv files with their first date -> reducing search time
mylist = list()  # contain all file names that are not empty
transactions = list()  # the output
sell_when = ''
min_date_sell = '2017-11-10'  # the first date after current a stock must be sold
end_date = '2017-11-10'  # the date all stocks must have been bought and sold
use_cols = ["Date", "High", "Low", "Volume"]
size_dict = {'Low': np.float32, 'High': np.float32, 'Volume': np.uint32}
current_name = ''


def reset_system(end_day='2017-11-10'):
    """ A function that deletes all transactions
        so system be ready for new tests"""
    global total_money, both_money, purchased, min_date, current_date, transactions, min_date_sell, end_date, current_name
    total_money = 1  # starting with 1$
    both_money = 1
    purchased = {}  # quantity,buy price,buy date,sell date
    min_date = '1970-01-01'
    current_date = '1962-06-25'
    transactions = list()  # the output
    min_date_sell = '1965-09-27'  # the first date after current a stock must be sold
    end_date = end_day  # the date all stocks must have been bought and sold
    buy(current_name, current_date, 'Low', min_date_sell)
    current_date = min_date_sell
    for stock in list(purchased.keys()):
        if worth_sell(stock):
            sell(current_date, stock, 'High')


def open_txt(stock_name):
    """ Takes the name of the stock
        and returns the pandas data frame"""
    return dates_dict[stock_name][1]


def chang_date(years_add=1200):
    """ Max date a stock must have
        begin selling"""
    global min_date, current_date, end_date
    now = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    min_date = str(now + datetime.timedelta(days=years_add)).split()[0]
    if min_date > end_date:
        min_date = end_date


def find_limit(start, limit):
    """ Max date a stock must have
        begin selling"""
    global end_date
    now = datetime.datetime.strptime(start, "%Y-%m-%d")
    result = str(now + datetime.timedelta(days=limit)).split()[0]
    if result > end_date:
        result = end_date
    return result


def find_min_date():
    """ Finds the first stock to sell.
        Must be called everytime after
        sell function"""
    global min_date_sell, purchased
    if len(purchased) > 0:
        min_date_sell = min(purchased[stock][3].split(sep='/')[0] for stock in purchased)
    else:
        min_date_sell = end_date


def buy_total(frame, date, code, sell_date):  # if i keep them it is more complicated
    """ Calculate the maximum Stocks
        can be bought and sold at sell date"""
    global total_money
    if frame.at[date, 'Volume'] > 0:  # Volume can't be zero
        max_amount = int(total_money // frame.at[date, code])  # total must be integer
        max_allowed = int(frame.at[date, 'Volume'] * 0.1)  # 10% limit
        max_sell = int(frame.at[sell_date, 'Volume'] * 0.1)  # 10% limit sell

        if max_amount < max_allowed:
            if max_amount < max_sell:
                return max_amount
            else:
                return max_sell
        else:
            if max_allowed < max_sell:
                return max_allowed
            else:
                return max_sell
    else:
        return 0


def worth_buy(stock, frame, date, code, thres=2.0, sell_limit=0):
    """ Checks if the stock
        is wotrth buying"""
    global keep_time, end_date, total_money
    over = find_limit(date, sell_limit)
    checking = frame.loc[date:over]
    if checking.empty:
        return False, 0, '', 0, 0  # anything at date limits
    if stock not in purchased:
        when_sell = ((checking.High / frame.at[date, code]).idxmax())
    else:
        possible_dates = purchased[stock][3].split(sep='/')
        other_sell = max(k for k in possible_dates)
        checking2 = checking.loc[other_sell:]
        if checking2.empty:
            return False, 0, '', 0, 0  # anything at date limits
        when_sell = (checking2.High / frame.at[date, code]).idxmax()
    buy_value = frame.at[date, code]
    sell_value = frame.at[when_sell, code]
    total = buy_total(frame, date, code, when_sell)
    ans = sell_value / buy_value  # mporw na exw sunartisi
    income = (sell_value - buy_value) * total
    if when_sell <= end_date:
        return ans >= thres, ans, when_sell, total, income
    else:
        return False, 0, '', 0, 0


def buy(stock_name, date, code, when_sell):
    """ Buy a stock, and make current day equal
        to date of bought """
    global total_money, purchased, current_date, transactions, min_date_sell, both_money
    frame = open_txt(stock_name)
    total = buy_total(frame, date, code, when_sell)
    if date >= current_date and total > 0:
        print(date, 'buy-' + code, stock_name.split(sep='.')[0].upper(), total)
        transactions.append((date, 'buy-' + code, stock_name.split(sep='.')[0].upper(), total))
        total_money -= total * frame.at[date, code]
        current_date = date
        if current_date > '1975-01-01':
            chang_date(90)  # changes the max date i look for stocks
        else:
            chang_date()
        if when_sell < min_date_sell:
            min_date_sell = when_sell
        if stock_name in purchased:  # maybe i already have bought some stocks
            when_sell1 = purchased[stock_name][3] + '/' + when_sell
            purchased[stock_name] = [(purchased[stock_name][0] + total), frame.at[date, code], date,
                                     when_sell1, frame.at[when_sell, 'High']]  # isws lista me dates
        else:
            purchased[stock_name] = [total, frame.at[date, code], date, when_sell, frame.at[when_sell, 'High']]
        return True
    else:
        return False


def sell_total(date, stock_name):
    """ Calculate the maximum Stocks
        can be sold"""
    global total_money, purchased
    frame = open_txt(stock_name)
    max_amount = purchased[stock_name][0]
    max_allowed = int(frame.at[date, 'Volume'] * 0.1)
    return max_amount if max_allowed > max_amount else max_allowed


def worth_sell(stock_name):  # che if i should sell
    global purchased, current_date
    if purchased[stock_name][3].split(sep='/')[0] == current_date:
        return True
    else:
        return False


def sell(date, stock_name, code):
    """ Sell a stock"""
    global total_money, purchased, current_date, transactions, min_date_sell
    frame = open_txt(stock_name)
    total = sell_total(date, stock_name)
    print(date, 'sell-' + code, stock_name.split(sep='.')[0].upper(), total)
    transactions.append((date, 'sell-' + code, stock_name.split(sep='.')[0].upper(), total))
    total_money += total * frame.at[date, code]
    if purchased[stock_name][0] == total:
        del purchased[stock_name]
    else:
        purchased[stock_name][0] -= total
        purchased[stock_name][3] = ''.join((purchased[stock_name][3].split(sep='/', maxsplit=1)[1::]))
    find_min_date()


def initialize():
    global current_date, file_names, current_name, dates_dict, mylist, sell_when, min_date_sell
    worthing = 1
    for stock in file_names:
        if getsize('C:/Users/tzagk/Downloads/Stocks/' + stock) > 0:
            frame = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/' + stock, dtype=size_dict, usecols=use_cols, header=0,
                                index_col=0)
            dates_dict[stock] = [frame.index[0], frame]  # store start_time
            mylist.append(stock)
            if frame.index[0] <= min_date:  #
                check_date = frame.head(365).Low.idxmin()
                if frame.head(365).Low.min() <= 1:  # find the min date at four months
                    ans, res, when_sell, total, income = worth_buy(stock, frame, check_date,
                                                                   code='Low', thres=1.2,
                                                                   sell_limit=1200)
                    if ans:
                        if res > worthing:  # check best worth
                            worthing = res
                            current_date = check_date  # set start date
                            current_name = stock
                            min_date_sell = when_sell


def find_something(threl=2.0, my_limit=150, far=365):
    """ Find stocks tha are worth buying"""
    global current_date, total_money, min_date, current_name, dates_dict, mylist, min_date_sell
    worthing = list()
    for stock in mylist:
        if dates_dict[stock][0] <= min_date:  # dont open all files
            frame = open_txt(stock)
            temp = frame.loc[current_date:end_date]
            if not temp.empty:
                mydate = temp.head(far).Low.idxmin()
                my_min = temp.head(far).Low.min()
                if total_money >= my_min > 0:  # find the min date at four months
                    ans, res, when_sell, total, income = worth_buy(stock, frame, mydate, 'Low',
                                                                   thres=threl,
                                                                   sell_limit=my_limit)
                    if ans:
                        if current_date > '2000-01-01':
                            if income > 3 * 10 ** 6:
                                worthing.append([mydate, stock, res, when_sell, total, income])
                        elif current_date > '1985-01-01':
                            if income > 1.5 * 10 ** 6:
                                worthing.append([mydate, stock, res, when_sell, total, income])
                        else:
                            worthing.append([mydate, stock, res, when_sell, total, income])
    if current_date > '1990-01-01':
        return sorted(worthing, key=itemgetter(0))
    elif current_date > '1985-01-01':
        return sorted(worthing, key=itemgetter(0))
    else:
        answer = sorted(worthing, key=itemgetter(5), reverse=True)
        return answer[::11]


def run_now():
    """Run tests"""
    global current_date, end_date, both_money, total_money, purchased, min_date_sell
    both_list = list()
    tran_list = list()
    while current_date < end_date:
        buyed = False
        selled = False
        founded = list()
        datetime.datetime.strptime(current_date, "%Y-%m-%d")
        c1 = datetime.datetime.strptime(current_date, "%Y-%m-%d")
        c2 = datetime.datetime.strptime(min_date_sell, "%Y-%m-%d")
        c = abs((c2 - c1).days)
        if (c == 0 and len(purchased) == 0) or c > 50:
            if current_date > '2010-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.4, 2200, 12)
            elif current_date > '2005-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.4, 3000, 8)
            elif current_date > '2000-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.5, 2000, 90)
            elif current_date > '1990-01-01':
                if total_money / both_money > 0.2:
                    founded = find_something(1.5, 370, 120)
            elif current_date > '1985-01-01':
                if total_money / both_money > 0.5:
                    founded = find_something(1.5, 400, 10)
            elif current_date > '1970-01-01':
                if total_money / both_money > 0.5:
                    founded = find_something(1.5, 200, 120)
            else:
                if total_money / both_money > 0.1:
                    founded = find_something(1.5, 700, 1200)
            for x in range(len(founded)):
                if current_date > '1990-01-01' and total_money / both_money < 0.05:
                    break
                elif current_date > '1970-01-01' and total_money / both_money < 0.1:
                    break
                if founded[x][0] <= min_date_sell:
                    if buy(founded[x][1], founded[x][0], 'Low', founded[x][3]):
                        buyed = True
        if (not buyed or len(founded) == 0) and len(purchased) > 0:
            if min_date_sell > current_date:
                current_date = min_date_sell
        for stock in list(purchased.keys()):
            if worth_sell(stock):
                sell(current_date, stock, 'High')
                selled = True
        both_money = sum(purchased[x][0] * purchased[x][4] for x in purchased) + total_money
        tran_list.append(datetime.datetime.strptime(current_date, "%Y-%m-%d"))
        both_list.append(both_money)
        if not buyed and len(purchased) == 0 and len(founded) == 0 and not selled:
            break
    plt.plot(tran_list, both_list)
    plt.show()
    print('Total transactions:', len(transactions))
    print('Total $ in billions:', total_money / 10 ** 9)
    print('Both money $ in billions:', both_money / 10 ** 9)


initialize()
data = open_txt(current_name)
buy(current_name, current_date, 'Low', min_date_sell)
current_date = min_date_sell
for key in list(purchased.keys()):
    if worth_sell(key):
        sell(current_date, key, 'High')
run_now()
