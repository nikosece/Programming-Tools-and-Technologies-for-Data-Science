from os import listdir
from os.path import isfile, join
from operator import itemgetter
import pandas as pd
import datetime
import numpy as np
from multiprocessing import Pool
import matplotlib.pyplot as plt
import time


def portofolio():
    """ Calculates the total worth of
        purchased stocks using close price
        at current date"""
    global current_date, fixed_frames, purchased
    return sum(fixed_frames[stock_name][current_date] * purchased[stock_name][0] for stock_name in purchased)


def convert_date(my_date):
    """ Converts a string to datetime format"""
    return datetime.datetime.strptime(my_date, "%Y-%m-%d")


def read_csv(filename):
    """Reads and return pandas dataframe for a specific file name"""
    frame = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/' + filename, dtype=size_dict, header=0,
                        index_col="Date", parse_dates=True)
    all_days = pd.date_range(frame.index.min(), frame.index.max(), freq='D')
    frame2 = frame.reindex(index=all_days, method='nearest')['Close']  # Close price for all days
    return [filename, frame, frame.index[0]], [filename, frame2]


def reduce_stocks(thres, date1, date2):
    """Exclude stocks with variance lower than
        thers limit, so the calculations be faster and
        more efficient"""
    global mylist, dates_dict
    newdict = {}
    for i in mylist:
        if dates_dict[i][0] <= date2:
            frame = dates_dict[i][1]
            frame = frame.loc[date1:date2]
            if frame.var().High > thres:
                newdict[i] = frame
    return newdict


def find_min_date():
    """ Finds the stock with the lower sell date.
        Must be called everytime after
        sell function"""
    global min_date_sell, purchased, sell_dict
    if len(purchased) > 0:
        min_date_sell = min(sell_dict.keys())  # sell_dict contains all planned sell dates
    else:
        min_date_sell = end_date


def buy_total(frame, date, buy_value, sell_date):
    """ Calculate the maximum Stocks
        can be bought and sold at sell date"""
    global total_money
    volume = frame.at[date, 'Volume']
    max_amount = int(total_money // buy_value)  # total must be integer
    max_allowed = int(volume * 0.1)  # 10% limit
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


def worth_buy(buy_value, stock_name, frame, date, code, thres=2.0):
    """ Checks if the stock
        is wotrth buying"""
    global keep_time, end_date, total_money, sell_dict
    if stock_name + str(date) in selling_test:      # Can't buy an sell the same stock in same date using Low/High!!
        return False, 0, '', 0, 0
    if stock_name in purchased:
        frame = frame.drop(purchased[stock_name][3], errors='ignore')   # exlude planned sell dates from search
    when_sell = frame.High.idxmax()                                     # sell date
    sell_value = frame.at[when_sell, code]                              # price at that day
    total = buy_total(frame, date, buy_value, when_sell)
    ans = sell_value / buy_value                                        # threshold
    income = (sell_value - buy_value) * total
    return ans >= thres, ans, when_sell, total, income


def buy(stock_name, date, code, when_sell):
    """ Buy a stock, and make current day equal
        to date of bought """
    global total_money, purchased, current_date, transactions, min_date_sell, both_money, sell_dict, selling_test, dates_dict
    frame = dates_dict[stock_name][1]       # get the frame from the dictionary
    price = frame.at[date, 'Low']
    total = buy_total(frame, date, price, when_sell)    # check total again, beacause total money may have changed
    if date >= current_date and total > 0:              # can't go to past second time or buy zero stocks
        print(date.date(), 'buy-low', stock_name.split(sep='.')[0].upper(), total)
        transactions.append(str(date.date()) + ' buy-low ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
        total_money -= total * frame.at[date, code]
        current_date = date
        selling_test[stock_name + str(when_sell)] = total       # selling_test helps to know the exact volume of sell for each sell date
        if when_sell in sell_dict:
            sell_dict[when_sell].append(stock_name)
        else:
            sell_dict[when_sell] = [stock_name]
        if when_sell < min_date_sell:
            min_date_sell = when_sell                           # check if there is s sooner sell date
        if stock_name in purchased:  # maybe i already have bought some stocks
            (purchased[stock_name][3]).append(when_sell)        # keep a list of sell dates
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
    total = selling_test[stock_name + str(date)]    # keep different totals for different dates by name+sell date
    del selling_test[stock_name + str(date)]
    print(date.date(), 'sell-high', stock_name.split(sep='.')[0].upper(), total)
    transactions.append(str(date.date()) + ' sell-high ' + stock_name.split(sep='.')[0].upper() + " " + str(total))
    total_money += total * frame.at[date, code]
    if purchased[stock_name][0] == total:
        del purchased[stock_name]
    else:
        purchased[stock_name][0] -= total
        purchased[stock_name][3].pop(0)     # if there are many sell dates, delete the most recent


def initialize():
    """ Reads all available data frames and saves
        them at a dictionary"""
    global dates_dict, mylist, fixed_frames
    if __name__ == '__main__':
        file_names = [stock_name for stock_name in listdir('C:/Users/tzagk/Downloads/Stocks') if
                      isfile(join('C:/Users/tzagk/Downloads/Stocks', stock_name))]
        with Pool(processes=12) as pool:  # number of processes
            df_list = pool.map(read_csv, file_names)
        list1 = df_list
        dates_dict = {i[0][0]: [i[0][2], i[0][1]] for i in list1}   # dafault frames
        fixed_frames = {i[1][0]: i[1][1] for i in list1}    # frames with all dates
        mylist = dates_dict.keys()


def find_something(threl=2.0, my_limit=150, far=365, mystocks=None):
    """ Find stocks tha are worth buying"""
    global current_date, total_money, min_date, current_name, dates_dict, mylist, min_date_sell, mylist2
    worthing = list()               # list with best stocks to buy
    min_date = current_date + datetime.timedelta(days=1200)     # max day a stock must have started else don't check
    far2 = current_date + datetime.timedelta(days=2 * far)      # max day to check for min low price
    if far2 > min_date_sell:
        far2 = min_date_sell
    if current_date <= convert_date('2005-01-01'):
        for stock_name in mylist2:
            if dates_dict[stock_name][0] <= min_date:  # dont open all files
                frame = dates_dict[stock_name][1]
                temp = frame.loc[current_date:far2]     # check only dates in seach window
                if not temp.empty:
                    mydate = temp.Low.idxmin()          # buy date
                    my_min = temp.at[mydate, 'Low']     # buy price
                    where = mydate + datetime.timedelta(days=my_limit)      # max sell date
                    frame = frame.loc[mydate:where]     # selling check window
                    if total_money >= my_min > 0 and frame.High.max() / my_min > threl:
                        ans, res, when_sell, total, income = worth_buy(my_min, stock_name, frame, mydate, 'Low',
                                                                       thres=threl)
                        if ans:
                            if current_date > convert_date('2000-01-01'):
                                if income > 3 * 10 ** 6:        # reject stocks with low income
                                    worthing.append([mydate, stock_name, res, when_sell, total, income])
                            elif current_date > convert_date('1985-01-01'):
                                if income > 1.5 * 10 ** 6:
                                    worthing.append([mydate, stock_name, res, when_sell, total, income])
                            else:
                                worthing.append([mydate, stock_name, res, when_sell, total, income])

    else:
        for stock_name in mystocks:
            if dates_dict[stock_name][0] <= current_date:
                frame = dates_dict[stock_name][1]
                temp = frame.loc[current_date:far2]
                if not temp.empty:
                    mydate = temp.Low.idxmin()
                    my_min = temp.at[mydate, 'Low']
                    where = mydate + datetime.timedelta(days=my_limit)
                    frame = frame.loc[mydate:where]
                    if total_money >= my_min > 0 and frame.High.max() / my_min > threl:
                        ans, res, when_sell, total, income = worth_buy(my_min, stock_name, frame, mydate, 'Low',
                                                                       thres=threl)
                        if ans and total > 100:
                            if income > 3 * 10 ** 6:
                                worthing.append([mydate, stock_name, res, when_sell, total, income])
    if current_date > convert_date('1985-01-01'):
        return sorted(worthing, key=itemgetter(0))
    else:
        answer = sorted(worthing, key=itemgetter(5), reverse=True)      # storted by income
        return answer[::11]     # at first buy just the most worthing stocks
    pass


def run_now():
    """Run the system"""
    global current_date, end_date, both_money, total_money, purchased, min_date_sell, sell_dict, reduced_stocks1, reduced_stocks2, dates_lists, total_list, portofolio_list
    date1 = convert_date('2010-01-01')
    date2 = convert_date('2005-01-01')
    date3 = convert_date('2000-01-01')
    date4 = convert_date('1990-01-01')
    date5 = convert_date('1985-01-01')
    date6 = convert_date('1970-01-01')
    date8 = convert_date('2017-03-01')
    last_buy = current_date
    while current_date <= end_date:
        buyed = False
        selled1 = False
        founded = list()
        c = abs((min_date_sell - current_date).days)    # days till next sell
        if (c == 0 and len(purchased) == 0) or c > 3:
            if current_date > date1:                    # different arguments for different dates
                founded = find_something(1.4, 2200, 12, reduced_stocks2)
            elif current_date > date2:
                founded = find_something(1.4, 2500, 8, reduced_stocks1)
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
            for re in range(len(founded)):
                if current_date > date4 and total_money / both_money < 0.05:
                    break                           # if money drop a lot it's not worth buying
                elif current_date > date6 and total_money / both_money < 0.1:
                    break
                if founded[re][0] <= min_date_sell:
                    if buy(founded[re][1], founded[re][0], 'Low', founded[re][3]):
                        buyed = True
                        last_buy = founded[re][0]
        else:                   # if 10 days without any buy, search!
            should = abs((current_date - last_buy).days)  # 10 days window
            if should >= 10 and date1 < current_date < date8:
                founded = find_something(1.4, 2200, c, reduced_stocks2)
                for re in range(len(founded)):
                    if founded[re][0] <= min_date_sell:
                        if buy(founded[re][1], founded[re][0], 'Low', founded[re][3]):
                            buyed = True
                            last_buy = founded[re][0]

        both_money = sum(purchased[y][0] * purchased[y][4] for y in purchased) + total_money
        if (not buyed or len(founded) == 0) and len(purchased) > 0:
            if min_date_sell > current_date:
                current_date = min_date_sell
        if current_date in sell_dict:       # check if current date is also sell date
            for stock_name in sell_dict[current_date]:  # sell dict contains a list of stocks for each sell date
                sell(current_date, stock_name, 'High')
                selled1 = True
            del sell_dict[current_date]
            find_min_date()                             # fix min date sell value
        dates_lists.append(current_date)                # save data when we have at least one transactions
        total_list.append(total_money)
        portofolio_list.append(portofolio() + total_money)
        current_date = current_date + datetime.timedelta(days=1)    # go to next day
        if not buyed and len(purchased) == 0 and len(founded) == 0 and not selled1:
            break
    print('Total transactions:', len(transactions))
    print('Total $ in billions:', total_money / 10 ** 9)
    with open('large.txt', 'w') as f:
        f.write(str(len(transactions)))
        f.write('\n')
        for item in transactions:
            f.write("%s\n" % item)

    dates_lists.pop()
    portofolio_list.pop()
    total_list.pop()
    d = {'Portofolio': portofolio_list, 'Balance': total_list}
    frame = pd.DataFrame(d, dates_lists)
    all_days = pd.date_range(frame.index.min(), frame.index.max(), freq='D')
    frame2 = frame.reindex(index=all_days, method='pad')
    portofolio_list = frame2['Portofolio'].values.tolist()
    total_list = frame2['Balance'].values.tolist()
    p2 = plt.bar(all_days, portofolio_list, 5.0, log='True', color='orange')
    p1 = plt.bar(all_days, total_list, 5.0, log='True', color='b')
    plt.title('Valuation')
    plt.legend((p1[0], p2[0]), ('Balance', 'Portofolio'))
    plt.savefig('Large.png')


total_money = 1  # starting with 1$
both_money = 1
purchased = {}  # quantity,buy price,buy date,sell date
min_date = convert_date('1970-01-01')
current_date = convert_date('1962-06-25')
keep_time = 120  # max time before sell,3 months !!! must be equal when searching
dates_dict = {}  # keeping all csv files with their first date -> reducing search time
fixed_frames = {}
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
dates_lists = list()
total_list = list()
portofolio_list = list()

if __name__ == '__main__':
    start = time.time()
    initialize()
    check = convert_date('2005-01-01')
    check2 = convert_date('2010-01-01')
    check3 = convert_date('2017-11-10')
    for x in mylist:
        if dates_dict[x][0] <= check:
            mylist2.append(x)
    data = dates_dict[current_name][1]
    buy(current_name, current_date, 'Low', min_date_sell)
    dates_lists.append(current_date)
    total_list.append(total_money)
    portofolio_list.append(portofolio() + total_money)
    current_date = min_date_sell
    if current_date in sell_dict:
        for stock in sell_dict[current_date]:
            sell(current_date, stock, 'High')
            selled = True
        del sell_dict[current_date]
    dates_lists.append(current_date)
    total_list.append(total_money)
    portofolio_list.append(portofolio() + total_money)
    find_min_date()
    reduced_stocks1 = reduce_stocks(40, check, check2)
    reduced_stocks2 = reduce_stocks(40, check2, check3)
    run_now()
    print('It took', (time.time() - start) / 60.0, 'minutes.')
