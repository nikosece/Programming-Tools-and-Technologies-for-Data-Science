from os import listdir
from os.path import isfile, join
from operator import itemgetter
import pandas as pd
import datetime
import numpy as np
from multiprocessing import Pool
import time
from pdb import set_trace
# import cProfile
# import matplotlib.pyplot as plt

total_money = 1  # starting with 1$
both_money = 1
purchased = {}  # quantity,buy price,buy date,sell date
min_date = '1970-01-01'
current_date = '1962-06-25'
keep_time = 120  # max time before sell,3 months !!! must be equal when searching
dates_dict = {}  # keeping all csv files with their first date -> reducing search time
mylist = list()  # contain all file names that are not empty
transactions = list()  # the output
sell_when = ''
sell_dict = {}
min_date_sell = '1965-09-27'  # the first date after current a stock must be sold
end_date = '2017-11-10'  # the date all stocks must have been bought and sold
use_cols = ["Date", "High", "Low", "Volume"]
size_dict = {'Low': np.float64, 'High': np.float64, 'Volume': np.uint64}
selling_test = {}
current_name = 'ge.us.txt'

def reset_system(when='2017-11-10'):
    global total_money, both_money, purchased, min_date, current_date, transactions, sell_dict, min_date_sell,selling_test, end_date
    total_money = 1  # starting with 1$
    both_money = 1
    purchased = {}  # quantity,buy price,buy date,sell date
    min_date = '1970-01-01'
    current_date = '1962-06-25'
    transactions = list()
    sell_dict = {}
    min_date_sell = '1965-09-27'
    selling_test = {}
    end_date = when
    buy(current_name, current_date, 'Low', min_date_sell)
    current_date = min_date_sell
    for key in list(purchased.keys()):
        if worth_sell(key):
            sell(current_date, key, 'High')
            del sell_dict[current_date]

def read_csv(filename):
    """converts a filename to a pandas dataframe"""
    frame = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/' + filename, dtype=size_dict, usecols=use_cols, header=0,
                        index_col=0)
    return [filename, frame, frame.index[0]]


def open_txt(stock_name):
    global dates_dict
    """ Takes the name of the stock
        and returns the pandas data frame"""
    return dates_dict[stock_name][1]


def reduce_stocks(thres=10):
    global mylist
    newdict = {}
    for x in mylist:
        frame = open_txt(x)
        frame = frame.loc['2005-01-01':]
        if frame.var().High > thres:
            newdict[x] = frame
    return newdict


def chang_date(years_add=1200):
    """ Max date a stock must have
        begin selling"""
    global min_date, current_date, end_date
    now = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    min_date = str(now + datetime.timedelta(days=years_add)).split()[0]
    if min_date > end_date:
        min_date = end_date


def find_limit(begin, limit):
    """ Max date a stock must have
        begin selling"""
    global end_date
    now = datetime.datetime.strptime(begin, "%Y-%m-%d")
    result = str(now + datetime.timedelta(days=limit)).split()[0]
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


def worth_buy(stock, frame, date, code, thres=2.0, sell_limit=0):
    """ Checks if the stock
        is wotrth buying"""
    global keep_time, end_date, total_money, sell_dict
    if stock + date in selling_test:
        return False, 0, '', 0, 0
    over = find_limit(date, sell_limit)
    if stock  in purchased:
        frame = frame.drop(purchased[stock][3].split(sep='/'))
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
    frame = open_txt(stock_name)
    total = buy_total(frame, date, code, when_sell)
    if date >= current_date and total > 0:  
        print(date, 'buy-low', stock_name.split(sep='.')[0].upper(), total)
        transactions.append(date + ' buy-low ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
        total_money -= total * frame.at[date, code]
        current_date = date
        selling_test[stock_name + when_sell] = total
        if when_sell in sell_dict:
            sell_dict[when_sell].append(stock_name)
        else:
            sell_dict[when_sell] = [stock_name]    
        if current_date > '1975-01-01':
            chang_date(90)  # changes the max date i look for stocks
        else:
            chang_date()
        if when_sell < min_date_sell:
            min_date_sell = when_sell
        if stock_name in purchased:  # maybe i already have bought some stocks
            when_sell1 = purchased[stock_name][3] + '/' + when_sell
            test_list = when_sell1.split(sep='/')
            test_list = sorted(test_list)
            when_sell1 = '/'.join(test_list)
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
    global total_money, purchased, current_date, transactions, min_date_sell, selling_test
    frame = open_txt(stock_name)
    total = selling_test[stock_name + date]
    del selling_test[stock_name + date]
    print(date, 'sell-high', stock_name.split(sep='.')[0].upper(), total)
    transactions.append(date + ' sell-high ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
    total_money += total * frame.at[date, code]
    if purchased[stock_name][0] == total:
        del purchased[stock_name]
    else:
        purchased[stock_name][0] -= total
        purchased[stock_name][3] = ''.join((purchased[stock_name][3].split(sep='/', maxsplit=1)[1::]))


def initialize():
    global dates_dict, mylist
    if __name__ == '__main__':
        file_names = [stock for stock in listdir('C:/Users/tzagk/Downloads/Stocks') if
                      isfile(join('C:/Users/tzagk/Downloads/Stocks', stock))]
        with Pool(processes=12) as pool:  # or whatever your hardware can support
            df_list = pool.map(read_csv, file_names)
        list1 = df_list
        dates_dict = {i[0]: [i[2], i[1]] for i in list1}
        mylist = dates_dict.keys()


def find_something(threl=2.0, my_limit=150, far=365, mystocks=None):
    """ Find stocks tha are worth buying"""
    global current_date, total_money, min_date, current_name, dates_dict, mylist, min_date_sell, reduced_stocks
    worthing = list()
    if current_date <= '2005-01-01':
        for stock in mylist:
            if dates_dict[stock][0] <= min_date:  # dont open all files
                frame = open_txt(stock)
                temp = frame.loc[current_date:min_date_sell]
                if not temp.empty:
                    mydate = temp.head(far).Low.idxmin()
                    if mydate <= min_date_sell:
                        my_min = temp.head(far).Low.min()
                        if total_money >= my_min > 0:  # find the min date at four months
                            ans, res, when_sell, total, income = worth_buy(stock, frame, mydate, 'Low',
                                                                           thres=threl,
                                                                           sell_limit=my_limit)
                            if ans:
                                if current_date > '2015-01-01':
                                    if total >= 1000:
                                        worthing.append([mydate, stock, res, when_sell, total, income])
                                elif current_date > '2000-01-01':
                                    if income > 3 * 10 ** 6:
                                        worthing.append([mydate, stock, res, when_sell, total, income])
                                elif current_date > '1985-01-01':
                                    if income > 1.5 * 10 ** 6:
                                        worthing.append([mydate, stock, res, when_sell, total, income])
                                else:
                                    worthing.append([mydate, stock, res, when_sell, total, income])
    else:
        for stock in mystocks:
            if dates_dict[stock][0] <= current_date:
                frame = reduced_stocks[stock]
                temp = frame.loc[current_date:min_date_sell]
                if not temp.empty:
                    mydate = temp.head(far).Low.idxmin()
                    if mydate <= min_date_sell:
                        my_min = temp.head(far).Low.min()
                        if total_money >= my_min > 0:  # find the min date at four months
                            ans, res, when_sell, total, income = worth_buy(stock, frame, mydate, 'Low', thres=threl,
                                                                           sell_limit=my_limit)
                            if ans and total > 100:
                                if income > 3 * 10 ** 6:
                                    worthing.append([mydate, stock, res, when_sell, total, income])
    if current_date > '1990-01-01':
        return sorted(worthing, key=itemgetter(0))
    elif current_date > '1985-01-01':
        return sorted(worthing, key=itemgetter(0))
    else:
        answer = sorted(worthing, key=itemgetter(5), reverse=True)
        return answer[::11]
    pass
                


def dokimes():
    list1 = [1.3,1.4,1.5]
    result = {}
    for x1 in list1:
        for x2 in range(200,1000,50):
            for x3 in range(10,90,10):
                reset_system()
                both_mon = run_now(x1,x2,x3)
                result[str(x1)+"-"+str(x2)+"-"+str(x3)] = both_money
    return result            
def run_now(x1=1.4,x2=370, x3=15):
    """Run tests"""
    global current_date, end_date, both_money, total_money, purchased, min_date_sell, sell_dict, reduced_stocks
    while current_date <= end_date:
        buyed = False
        selled = False
        my_ok = 0
        founded = list()
        c1 = datetime.datetime.strptime(current_date, "%Y-%m-%d")
        c2 = datetime.datetime.strptime(min_date_sell, "%Y-%m-%d")
        c = abs((c2 - c1).days)
        if (c == 0 and len(purchased) == 0) or c > 5:
            if current_date > '2010-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.4, 2200, 12, reduced_stocks)
            elif current_date > '2005-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.4, 2500, 8, reduced_stocks)
            elif current_date > '2000-01-01':
                if total_money / both_money > 0.1:
                    founded = find_something(1.5, 500, 10)
            elif current_date > '1990-01-01':
                if total_money / both_money > 0.2:
                    founded = find_something(1.5, 290, 15)
            elif current_date > '1985-01-01':
                if total_money / both_money > 0.5:
                    founded = find_something(1.3, 200, 110)
            elif current_date > '1970-01-01':
                if total_money / both_money > 0.5:
                    founded = find_something(1.3, 450, 90)
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
                        my_ok += 1
        # else:
        #     if current_date > '2013-01-01' and len(purchased) < 500:
        #         founded = find_something(1.3, 2200, c + 1, reduced_stocks)
        #         # set_trace()
        #     for x in range(len(founded)):
        #         if buy(founded[x][1], founded[x][0], 'Low', founded[x][3]):
        #             buyed = True
        #             my_ok += 1
        both_money = sum(purchased[x][0] * purchased[x][4] for x in purchased) + total_money
        if (not buyed or len(founded) == 0) and len(purchased) > 0:
            if min_date_sell > current_date:
                current_date = min_date_sell
        if current_date in sell_dict:
            for stock in sell_dict[current_date]:
                if stock in purchased:
                    sell(current_date, stock, 'High')
                    selled = True
            del sell_dict[current_date]
            find_min_date()
            # if current_date > '1975-01-01':
            #     chang_date(90)  # changes the max date i look for stocks
            # else:
            #     chang_date()
            # current_date=find_limit(current_date,1)
            if current_date > '2000-01-01':
                chang_date(200)  # changes the max date i look for stocks
            else:
                chang_date()     
        current_date = find_limit(current_date, 1)
        if not buyed and len(purchased) == 0 and len(founded) == 0 and not selled:
            break
        # if current_date >= "2005-01-01":
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


if __name__ == '__main__':
    initialize()
    data = open_txt(current_name)
    buy(current_name, current_date, 'Low', min_date_sell)
    current_date = min_date_sell
    for key in list(purchased.keys()):
        if worth_sell(key):
            sell(current_date, key, 'High')
            del sell_dict[current_date]
    find_min_date()
    reduced_stocks = reduce_stocks(40)
    start = time.time()
    run_now()
    print('It took', time.time() - start, 'seconds.')
