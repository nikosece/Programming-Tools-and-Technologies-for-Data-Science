from os import listdir
from os.path import isfile, join
from operator import itemgetter
import pandas as pd
import datetime
import numpy as np
from multiprocessing import Pool
import time


# import cProfile
# import matplotlib.pyplot as plt


def reset_system(when='2017-11-10'):
    global total_money, both_money, purchased, min_date, current_date, transactions, sell_dict, min_date_sell, selling_test, end_date
    total_money = 1  # starting with 1$
    both_money = 1
    purchased = {}  # quantity,buy price,buy date,sell date
    min_date = convert_date('1970-01-01')
    current_date = convert_date('1962-06-25')
    transactions = list()
    sell_dict = {}
    min_date_sell = convert_date('1965-09-27')
    selling_test = {}
    end_date = when
    buy(current_name, current_date, 'Low', min_date_sell, 1)
    current_date = min_date_sell
    sell(current_date, current_name, 'High')
    del sell_dict[current_date]


def convert_date(my_date):
    return datetime.datetime.strptime(my_date, "%Y-%m-%d")


def read_csv(filename):
    """converts a filename to a pandas dataframe"""
    frame = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/' + filename, dtype=size_dict, usecols=use_cols, header=0,
                        index_col="Date", parse_dates=True)
    return [filename, frame, frame.index[0]]


def reduce_stocks(thres=40):
    """Exclude stocks with variance lower than
        thers, so the calculations be faster and
        more efficient"""
    global mylist, dates_dict
    newdict = {}
    my_date = convert_date('2005-01-01')
    for x in mylist:
        frame = dates_dict[x][1]
        frame = frame.loc[my_date:]
        if frame.var().High > thres:
            newdict[x] = frame
    return newdict


def find_limit(begin, limit):
    """ Max date a stock must have
        begin selling"""
    global end_date
    result = begin + datetime.timedelta(days=limit)
    if result > end_date:
        result = end_date
    return result


def find_min_date():
    """ Finds the first stock to sell.
        Must be called everytime after
        sell function"""
    global min_date_sell, purchased, sell_dict
    if len(purchased) > 0:
        min_date_sell = min(sell_dict.keys())
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


def worth_buy(stock_name, frame, date, code, thres=2.0, sell_limit=0):
    """ Checks if the stock
        is wotrth buying"""
    global keep_time, end_date, total_money, sell_dict
    if stock_name + str(date) in selling_test:
        return False, 0, '', 0, 0
    over = find_limit(date, sell_limit)
    if stock_name in purchased:
        frame = frame.drop(purchased[stock_name][3])
    checking = frame.loc[date:over]
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


def buy(stock_name, date, code, when_sell):
    """ Buy a stock, and make current day equal
        to date of bought """
    global total_money, purchased, current_date, transactions, min_date_sell, both_money, sell_dict, selling_test, dates_dict
    frame = dates_dict[stock_name][1]
    total = buy_total(frame, date, code, when_sell)
    if date >= current_date and total > 0:
        print(date.date(), 'buy-low', stock_name.split(sep='.')[0].upper(), total)
        transactions.append(str(date.date()) + ' buy-low ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
        total_money -= total * frame.at[date, code]
        current_date = date
        selling_test[stock_name + str(when_sell)] = total
        if when_sell in sell_dict:
            sell_dict[when_sell].append(stock_name)
        else:
            sell_dict[when_sell] = [stock_name]
        if when_sell < min_date_sell:
            min_date_sell = when_sell
        if stock_name in purchased:  # maybe i already have bought some stocks
            (purchased[stock_name][3]).append(when_sell)
            when_sell1 = sorted(purchased[stock_name][3])
            purchased[stock_name] = [(purchased[stock_name][0] + total), frame.at[date, code], date,
                                     when_sell1,
                                     frame.at[when_sell, 'High']]  # isws lista me dates
        else:
            purchased[stock_name] = [total, frame.at[date, code], date, [when_sell], frame.at[when_sell, 'High']]
        return True
    else:
        return False


def sell(date, stock_name, code):
    """ Sell a stock"""
    global total_money, purchased, current_date, transactions, min_date_sell, selling_test, dates_dict
    frame = dates_dict[stock_name][1]
    total = selling_test[stock_name + str(date)]
    del selling_test[stock_name + str(date)]
    print(date.date(), 'sell-high', stock_name.split(sep='.')[0].upper(), total)
    transactions.append(str(date.date()) + ' sell-high ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
    total_money += total * frame.at[date, code]
    if purchased[stock_name][0] == total:
        del purchased[stock_name]
    else:
        purchased[stock_name][0] -= total
        purchased[stock_name][3].pop(0)


def initialize():
    global dates_dict, mylist
    if __name__ == '__main__':
        file_names = [stock_name for stock_name in listdir('C:/Users/tzagk/Downloads/Stocks') if
                      isfile(join('C:/Users/tzagk/Downloads/Stocks', stock_name))]
        with Pool(processes=12) as pool:  # or whatever your hardware can support
            df_list = pool.map(read_csv, file_names)
        list1 = df_list
        dates_dict = {i[0]: [i[2], i[1]] for i in list1}
        mylist = dates_dict.keys()


def find_something(threl=2.0, my_limit=150, far=365, mystocks=None):
    """ Find stocks tha are worth buying"""
    global current_date, total_money, min_date, current_name, dates_dict, mylist, min_date_sell, reduced_stocks, mylist2
    worthing = list()
    min_date = current_date + datetime.timedelta(days=1200)
    if current_date <= convert_date('2005-01-01'):
        for stock_name in mylist2:
            if dates_dict[stock_name][0] <= min_date:  # dont open all files
                frame = dates_dict[stock_name][1]
                temp = frame.loc[current_date:min_date_sell]
                if not temp.empty:
                    mydate = temp.head(far).Low.idxmin()
                    if mydate <= min_date_sell:
                        my_min = temp.head(far).Low.min()
                        if total_money >= my_min > 0:  # find the min date at four months
                            ans, res, when_sell, total, income = worth_buy(stock_name, frame, mydate, 'Low',
                                                                           thres=threl,
                                                                           sell_limit=my_limit)
                            if ans:
                                if current_date > convert_date('2000-01-01'):
                                    if income > 3 * 10 ** 6:
                                        worthing.append([mydate, stock_name, res, when_sell, total, income])
                                elif current_date > convert_date('1985-01-01'):
                                    if income > 1.5 * 10 ** 6:
                                        worthing.append([mydate, stock_name, res, when_sell, total, income])
                                else:
                                    worthing.append([mydate, stock_name, res, when_sell, total, income])
    else:
        for stock_name in mystocks:
            if dates_dict[stock_name][0] <= current_date:
                frame = reduced_stocks[stock_name]
                temp = frame.loc[current_date:min_date_sell]
                if not temp.empty:
                    mydate = temp.head(far).Low.idxmin()
                    if mydate <= min_date_sell:
                        my_min = temp.head(far).Low.min()
                        if total_money >= my_min > 0:  # find the min date at four months
                            ans, res, when_sell, total, income = worth_buy(stock_name, frame, mydate, 'Low',
                                                                           thres=threl,
                                                                           sell_limit=my_limit)
                            if ans and total > 100:
                                if income > 3 * 10 ** 6:
                                    worthing.append([mydate, stock_name, res, when_sell, total, income])
    if current_date > convert_date('1990-01-01'):
        return sorted(worthing, key=itemgetter(0))
    elif current_date > convert_date('1985-01-01'):
        return sorted(worthing, key=itemgetter(0))
    else:
        answer = sorted(worthing, key=itemgetter(5), reverse=True)
        return answer[::11]
    pass


def run_now():
    """Run tests"""
    global current_date, end_date, both_money, total_money, purchased, min_date_sell, sell_dict, reduced_stocks
    date1 = convert_date('2010-01-01')
    date2 = convert_date('2005-01-01')
    date3 = convert_date('2000-01-01')
    date4 = convert_date('1990-01-01')
    date5 = convert_date('1985-01-01')
    date6 = convert_date('1970-01-01')

    while current_date <= end_date:
        buyed = False
        selled1 = False
        founded = list()
        c = abs((min_date_sell - current_date).days)
        if (c == 0 and len(purchased) == 0) or c > 5:
            if current_date > date1:
                founded = find_something(1.4, 2200, 12, reduced_stocks)
            elif current_date > date2:
                founded = find_something(1.4, 2500, 8, reduced_stocks)
            elif current_date > date3:
                if total_money / both_money > 0.1:
                    founded = find_something(1.5, 500, 10)
            elif current_date > date4:
                if total_money / both_money > 0.2:
                    founded = find_something(1.5, 290, 15)
            elif current_date > date5:
                if total_money / both_money > 0.5:
                    founded = find_something(1.3, 200, 110)
            elif current_date > date6:
                if total_money / both_money > 0.5:
                    founded = find_something(1.3, 450, 90)
            else:
                if total_money / both_money > 0.1:
                    founded = find_something(1.5, 700, 1200)
            for x in range(len(founded)):
                if current_date > date4 and total_money / both_money < 0.05:
                    break
                elif current_date > date6 and total_money / both_money < 0.1:
                    break
                if founded[x][0] <= min_date_sell:
                    if buy(founded[x][1], founded[x][0], 'Low', founded[x][3]):
                        buyed = True
        # else:
        #     if current_date > '2013-01-01':  # and len(purchased) < 500:
        #         set_trace()
        # #         founded = find_something(1.3, 2200, c + 1, reduced_stocks)
        # #         # set_trace()
        # #     for x in range(len(founded)):
        # #         if buy(founded[x][1], founded[x][0], 'Low', founded[x][3]):
        # #             buyed = True
        both_money = sum(purchased[x][0] * purchased[x][4] for x in purchased) + total_money
        if (not buyed or len(founded) == 0) and len(purchased) > 0:
            if min_date_sell > current_date:
                current_date = min_date_sell
        if current_date in sell_dict:
            for stock_name in sell_dict[current_date]:
                sell(current_date, stock_name, 'High')
                selled1 = True
            del sell_dict[current_date]
            find_min_date()
        current_date = find_limit(current_date, 1)
        if not buyed and len(purchased) == 0 and len(founded) == 0 and not selled1:
            break
        # if current_date >= date2
        #     # return both_money
        #     break

    print('Total transactions:', len(transactions))
    print('Total $ in billions:', total_money / 10 ** 9)
    print('Both money $ in billions:', both_money / 10 ** 9)
    with open('data3.txt', 'w') as f:
        f.write(str(len(transactions)))
        f.write('\n')
        for item in transactions:
            f.write("%s\n" % item)


total_money = 1  # starting with 1$
both_money = 1
purchased = {}  # quantity,buy price,buy date,sell date
min_date = convert_date('1970-01-01')
current_date = convert_date('1962-06-25')
keep_time = 120  # max time before sell,3 months !!! must be equal when searching
dates_dict = {}  # keeping all csv files with their first date -> reducing search time
mylist = list()  # contain all file names that are not empty
transactions = list()  # the output
sell_when = ''
sell_dict = {}
min_date_sell = convert_date('1965-09-27')  # the first date after current a stock must be sold
end_date = convert_date('2017-11-10')  # the date all stocks must have been bought and sold
use_cols = ["Date", "High", "Low", "Volume"]
size_dict = {'Low': np.float64, 'High': np.float64, 'Volume': np.uint64}
selling_test = {}
current_name = 'ge.us.txt'
mylist2 = list()

if __name__ == '__main__':
    start = time.time()
    initialize()
    check = convert_date('2005-01-01')
    for x in mylist:
        if dates_dict[x][0]<=check:
            mylist2.append(x)
    data = dates_dict[current_name][1]
    buy(current_name, current_date, 'Low', min_date_sell)
    current_date = min_date_sell
    if current_date in sell_dict:
        for stock in sell_dict[current_date]:
            sell(current_date, stock, 'High')
            selled = True
        del sell_dict[current_date]
    find_min_date()
    reduced_stocks = reduce_stocks(40)
    run_now()
    print('It took', (time.time() - start) / 60.0, 'minutes.')
