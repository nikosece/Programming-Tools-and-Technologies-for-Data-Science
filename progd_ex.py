from os import listdir
from os.path import isfile, join, getsize
from operator import itemgetter
import pandas as pd
import datetime
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
min_date_sell = ''  # the first date after current a stock must be sold
end_date = '2000-01-01'  # the date all stocks must have been bought and sold
current_name = ''


def open_txt(stock_name):
    """ Takes the name of the stock
        and returns the pandas data frame"""
    return dates_dict[stock_name][1]


def chang_date(years_add=900):
    """ Max date a stock must have
        begin selling"""
    global min_date, current_date, end_date
    now = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    min_date = str(now + datetime.timedelta(days=years_add)).split()[0]
    if min_date > end_date:
        min_date = end_date


def find_min_date():
    """ Finds the first stock to sell.
        Must be called everytime after
        sell function"""
    global min_date_sell, purchased
    if len(purchased) > 0:
        min_date_sell = min(purchased[stock][3] for stock in purchased)
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


def worth_buy(frame, date, code, thres=2.0, sell_limit=0):
    """ Checks if the stock
        is wotrth buying"""
    global keep_time, end_date, total_money
    if sell_limit == 0:
        sell_limit = keep_time
    if frame.at[date, code] > 0:  # price can't be zero
        checking = frame[(frame.index > date) & (frame.index <= end_date)].head(sell_limit)
        if checking.empty:
            return False, 0, '', 0, 0  # anything at date limits
        when_sell = ((checking.High / frame.at[date, code]).idxmax())
        buy_value = frame.at[date, code]
        sell_value = frame.at[when_sell, code]
        total = buy_total(frame, date, code, when_sell)
        ans = sell_value / buy_value  # mporw na exw sunartisi
        income = (sell_value - buy_value) * total
        if when_sell <= end_date:
            return ans >= thres, ans, when_sell, total, income
        else:
            return False, 0, '', 0, 0
    else:
        return False, 0, '', 0, 0


def buy(stock_name, date, code, when_sell):
    """ Buy a stock, and make current day equal
        to date of bought """
    global total_money, purchased, current_date, transactions, min_date_sell, both_money
    frame = open_txt(stock_name)
    total = buy_total(frame, date, code, when_sell)
    if date >= current_date and total > 0:
        if both_money > 10**6 and total < 10:
            return False
        if current_date > '1975-01-01' and total < 40:
            return False
        print(date, 'buy-' + code, stock_name.split(sep='.')[0].upper(), total)
        transactions.append((date, 'buy-' + code, stock_name.split(sep='.')[0].upper(), total))
        total_money -= total * frame.at[date, code]
        current_date = date
        chang_date()  # changes the max date i look for stocks
        if when_sell < min_date_sell:
            min_date_sell = when_sell
        if stock_name in purchased:  # maybe i already have bought some stocks
            purchased[stock_name] = [(purchased[stock_name][0] + total), frame.at[date, code], date,
                                     when_sell]  # isws lista me dates
        else:
            purchased[stock_name] = [total, frame.at[date, code], date, when_sell, frame.at[when_sell, 'High']]
        return True
    else:
        return False


def best_sell_date(stock_name, date, code):
    """ This is only if i am buying Stocks
        that i have already"""
    global current_date, keep_time, end_date
    frame = open_txt(stock_name)
    checking = frame[(frame.index > date) & (frame.index <= end_date)].head(keep_time)
    if checking.empty:
        return current_date
    when_sell = ((checking.High / frame.at[current_date, code]).idxmax())
    return when_sell


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
    if purchased[stock_name][3] == current_date:
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
        purchased[stock_name][3] = best_sell_date(stock_name, current_date, code)
    find_min_date()


def initialize():
    """ Since there is only one stock that i can
        buy at date limits, i can avoid all data.head(365)-> worth buy is only called one time (profiled code)
        compares to minimize init time"""
    global current_date, file_names, current_name, dates_dict, mylist, sell_when, min_date_sell
    worthing = 1
    for stock in file_names:
        if getsize('C:/Users/tzagk/Downloads/Stocks/' + stock) > 0:
            data = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/' + stock, header=0, index_col=0)
            dates_dict[stock] = [data.index[0], data]  # store start_time
            mylist.append(stock)
            if data.index[0] <= min_date:  #
                if data.head(365).Low.min() <= 1:  # find the min date at first year
                    ans, res, when_sell, total, income = worth_buy(data, data.head(365).Low.idxmin(), 'Low', thres=1.2,
                                                                   sell_limit=900)
                    if ans:
                        if res > worthing:  # check best worth
                            worthing = res
                            current_date = data.head(365).Low.idxmin()  # set start date
                            current_name = stock
                            min_date_sell = when_sell


def find_something(threl=2.0, my_limit=150, far=365):  # find an oportunity and return name and date
    global current_date, total_money, min_date, current_name, dates_dict, mylist, min_date_sell
    worthing = list()
    for stock in mylist:
        if dates_dict[stock][0] <= min_date and stock not in purchased:  # dont open all files
            frame = open_txt(stock)
            temp = frame[(frame.index > current_date) & (frame.index <= end_date) & (frame.index <= min_date_sell)]
            if temp.head(far).Low.min() <= total_money:  # find the min date at four months
                ans, res, when_sell, total, income = worth_buy(frame, temp.head(far).Low.idxmin(), 'Low', thres=threl,
                                                               sell_limit=my_limit)
                if ans:
                    worthing.append([temp.head(far).Low.idxmin(), stock, res, when_sell, total, income])
    return sorted(worthing, key=itemgetter(5), reverse=True)  # return only a par


initialize()
data = open_txt(current_name)
money_list = list()
both_list = list()
tran_list = list()
buy(current_name, current_date, 'Low', min_date_sell)
current_date = min_date_sell
for key in list(purchased.keys()):
    if worth_sell(key):
        sell(current_date, key, 'High')
while current_date < end_date:
    buyed = False
    selled = False
    # if both_money - total_money < 0.8*10**6:
    # if current_date > '2000-01-01':
    # founded = find_something(1.2, 950, 10)
    if current_date > '1990-01-01':
        if total_money/both_money > 0.1:
            founded = find_something(1.2, 950, 90)
    elif current_date > '1975-01-01':
        founded = find_something(1.2, 350, 90)
    else:
        founded = find_something(1.2, 500, 240)
    for x in range(len(founded)):
        if founded[x][0] <= min_date_sell:
            if buy(founded[x][1], founded[x][0], 'Low', founded[x][3]):
                buyed = True
    else:
        founded = list()
    if (not buyed or len(founded) == 0) and len(purchased) > 0:
        if min_date_sell > current_date:
            current_date = min_date_sell
    for key in list(purchased.keys()):
        if worth_sell(key):
            sell(current_date, key, 'High')
            selled = True
    money_list.append(total_money / 10 ** 9)
    both_money = sum(purchased[x][0] * purchased[x][4] for x in purchased) + total_money
    tran_list.append(len(transactions))
    both_list.append(both_money / 10 ** 9)
    if not buyed and len(purchased) == 0 and len(founded) == 0 and not selled:
        break
print('Total $ in billions:', total_money / 10 ** 9)
plt.plot(tran_list, both_list)
plt.show()
