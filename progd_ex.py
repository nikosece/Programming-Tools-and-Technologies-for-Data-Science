from os import listdir
from os.path import isfile, join,getsize
from operator import itemgetter
import pandas as pd

total_money = 1                 # starting with 1$
purchased = {}                  # quantity,buy price,buy date,sell date
min_date = '1970-01-01'
current_date = '1970-01-01'
keep_time = 1465                # max time before sell,4 years
file_names = [f for f in listdir('C:/Users/tzagk/Downloads/Stocks') if isfile(join('C:/Users/tzagk/Downloads/Stocks', f))]
dates_dict = {}                 # keeping all csv files with their first date -> reducing search time
mylist = list()                 # contain all file names that are not empty
transactions = list()           # the output
shell_when = ''
min_date_sell = ''              # the first date after current a stock must be sold
end_date = '2000-01-01'         # the date all stocks must have been bought and sold

def open_txt(name):
    ''' Takes the name of the stock
        and returns the pandas data frame'''
    return dates_dict[name][1]

def buy_total(frame,date,code):
    ''' Calculate the maximum Stocks
        can be bought'''
    global total_money
    if frame.at[date,'Volume'] > 0 :                    # Volume can't be zero
        max_amount = int(total_money // frame.at[date,code])    # total must be integer
        max_allowed = int( frame.at[date,'Volume'] * 0.1)       # 10% limit
        return max_amount if max_allowed >= max_amount else max_allowed
    else:
        return 0

def buy(name,date,code,when_sell):
    ''' Buy a stock, and make current day equal
        to date of bought and returns true or False
        depending on success'''
    global total_money,purchased,current_date,transactions
    if date >= current_date:    # Day of transaction must be >= of current date
        frame = open_txt(name)
        total = buy_total(frame,date,code)  # code is Low, or open
        if(total>0):
            print (date,'buy-'+code,name.split(sep='.')[0].upper(),total)
            transactions.append((date,'buy-'+code,name.split(sep='.')[0].upper(),total))
            total_money -= total * frame.at[date,code]
            current_date = date
            chang_date()    # changes the max date i look for stocks
            if name in purchased:   # maybe i already have bought some stocks
                purchased[name] = [(purchased[name][0] + total),frame.at[date,code],date,when_sell]
                find_min_date()
                return True
            else:
                purchased[name] = [total,frame.at[date,code],date,when_sell]
                find_min_date()
                return True
        else:
            return False
    else:
        return False

def best_sell_date(name, date):
    global current_date,keep_time
    frame = open_txt(name)
    checking=frame[(frame.index>date) & (frame.index<= date)].head(keep_time)
    if checking.empty: return '2020-12-30'  # date out of limits
    when_sell=((checking.High/frame.at[current_date,code]).idxmax())
    return when_sell

def sell_total(date,name):
    ''' Calculate the maximum Stocks
        can be sold'''
    global total_money, purchased
    frame = open_txt(name)
    max_amount = purchased[name][0]
    max_allowed = int( frame.at[date,'Volume'] * 0.1)
    return max_amount if max_allowed > max_amount else max_allowed

def sell(date,name,code):               # prepei na dw ti ginetai an den
    ''' Sell a stock
                    '''                 #tis poulisw oles (min_date_sell)
    global total_money,purchased,current_date,transactions,min_date_sell
    frame = open_txt(name)
    total = sell_total(date,name)
    print (date,'sell-'+code,name.split(sep='.')[0].upper(),total)
    transactions.append((date,'sell-'+code,name.split(sep='.')[0].upper(),total))
    total_money += total * frame.at[date,code]
    if purchased[name][0] == total:
        del purchased[name]
    else:
        purchased[name][0] -= total
        purchased[name][3] = best_sell_date(name,current_date)

    find_min_date()
def worth_buy(frame,date,code,thres=1.5):              #thelei ftiaksimo wste na pairnei sosto date, oxi current
    ''' checks if the stock
        is wotrth buying'''
    global keep_time,end_date
    if(frame.at[date,code]>0):                      # price can't be zero
        checking=frame[(frame.index>date) & (frame.index<= end_date)].head(keep_time)
        if checking.empty: return False,0,'' # anything at date limits
        ans=((checking.High/frame.at[date,code]).max())     # mporw na exw sunartisi
        when_sell=((checking.High/frame.at[date,code]).idxmax())
        if(when_sell<=end_date):
            return (checking.High/frame.at[date,code]).max() >= thres,ans,when_sell
        else:
            return False,0,''
    else:
        return False,0,''

def worth_sell(name):  # che if i should sell
    global purchased,current_date
    if purchased[name][3] == current_date:
        return True
    else:
        return False


def chang_date():
    global min_date,current_date
    try:
        min_date = indexing_list[indexing_list.index(current_date)+365]    # 1 year
    except:
        min_date = '2017-10-11'
def find_min_date():
    global min_date_sell,purchased
    if len(purchased)>0:
        for key in purchased:
            if min_date_sell not in purchased[key]:
                min_date_sell = purchased[key][3]
            if min_date_sell > purchased[key][3]:
                min_date_sell = purchased[key][3]
    else:
        min_date_sell = end_date

def initialize():
    global current_date,name,data,file_names,current_name,dates_dict,mylist,sell_when,min_date_sell
    worthing=1
    for x in file_names:
        if getsize('C:/Users/tzagk/Downloads/Stocks/'+x)>0:
            data = pd.read_csv('C:/Users/tzagk/Downloads/Stocks/'+x, header = 0, index_col=0)
            dates_dict[x] = [data.index[0],data]               # store start_time
            mylist.append(x)
            if data.index[0] <= min_date:           #
                if data.head(120).Low.min() <=1:       # find the min date at four months
                    ans,res,when_sell=worth_buy(data,data.head(120).Low.idxmin(),'Low',thres=1.2) # check worth
                    if ans:
                        if res > worthing:      # check best worth
                            worthing = res
                            current_date = data.head(120).Low.idxmin()  # set start date
                            current_name = x
                            min_date_sell = when_sell


def find_something(threl=1.2):           # find an oportunity and return name and date
    global current_date,total_money,min_date,current_name,dates_dict,mylist,min_date_sell
    worthing = list()
    temp_date = current_date
    for x in mylist:
        if dates_dict[x][0] <= min_date and x not in purchased:       # dont open all files
            data = open_txt(x)
            if data[(data.index>current_date) & (data.index<= end_date)].head(240).Low.min() <= total_money:       # find the min date at four months
                ans,res,when_sell=worth_buy(data,data[(data.index>current_date) & (data.index<= end_date)].head(240).Low.idxmin(),'Low',thres=threl) # check worth
                if ans:
                    worthing.append([data[(data.index>current_date) & (data.index<= end_date)].head(240).Low.idxmin(),x,res,when_sell])

    return sorted(worthing, key=itemgetter(2),reverse=True)



initialize()
data = open_txt(current_name)
indexing_list = data.index.values.tolist()      # in this way i can control dates

buy(current_name,current_date,'Low','1965-09-27')
current_date = '1965-09-27'
for key in list(purchased.keys()):
    if worth_sell(key):
        sell(current_date,key,'High')
while current_date <= end_date:
    if(total_money>500000):
        founded=find_something(threl=30)
    else:
        founded=find_something()
    buyed = False
    for x in founded:
        if x[3] <= min_date_sell:
            buyed = buy(x[1],x[0],'Low',x[3])
    if not buyed or len(founded)==0:
        current_date = min_date_sell
    for key in list(purchased.keys()):
        if worth_sell(key):
            sell(current_date,key,'High')
print(total_money)
