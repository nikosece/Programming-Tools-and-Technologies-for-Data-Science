from os import listdir
from os.path import isfile, join,getsize
from operator import itemgetter
import pandas as pd

total_money = 1
purchased = {}              # quantity, buy price, date
min_date = '1970-01-01'
current_date = '1970-01-01'
current_name =''
keep_time = 1465         # max time before sell,4 years
file_names = [f for f in listdir('C:/Users/tzagk/Downloads/Stocks') if isfile(join('C:/Users/tzagk/Downloads/Stocks', f))]
dates_dict = {}
mylist = list()
transactions = list()
shell_when = ''
min_date_sell = ''

def open_txt(name):
    return dates_dict[name][1]

def buy_total(frame,date,code):
    global total_money
    if frame.at[date,'Volume'] > 0 :            # Volume can't be zero
        max_amount = int(total_money // frame.at[date,code])
        max_allowed = int( frame.at[date,'Volume'] * 0.1)
        return max_amount if max_allowed > max_amount else max_allowed
    else:
        return 0

def buy(name,date,code,when_sell):
    global total_money,purchased,current_date,transactions
    if date >= current_date:
        frame = open_txt(name)
        total = buy_total(frame,date,'Low')
        if(total>0):
            print (date,'buy-'+code,name.split(sep='.')[0].upper(),total)
            transactions.append((date,'buy-'+code,name.split(sep='.')[0].upper(),total))
            total_money -= total * frame.at[date,code]
            current_date = date
            chang_date()
            if name in purchased:
                purchased[name] = [(purchased[name][0] + total),frame.at[date,code],date,when_sell]
                return True
            else:
                purchased[name] = [total,frame.at[date,code],date,when_sell]
                return True
        else:
            return False
    else:
        return False

def sell_total(frame,date,name):
    global total_money
    global purchased
    max_amount = purchased[name][0]
    max_allowed = int( frame.at[date,'Volume'] * 0.1)
    return max_amount if max_allowed > max_amount else max_allowed

def sell(date,name,code):
        global total_money,purchased,current_date,transactions
        frame = open_txt(name)
        total = sell_total(frame,date,name)
        print (date,'sell-'+code,name.split(sep='.')[0].upper(),total)
        transactions.append((date,'sell-'+code,name.split(sep='.')[0].upper(),total))
        total_money += total * frame.at[date,code]
        if purchased[name][0] == total:
            del purchased[name]
        else:
            purchased[name][0] -= total

def worth_buy(frame,date,code,thres=1.5):              #thelei ftiaksimo wste na pairnei sosto date, oxi current
    global keep_time
    if(frame.at[date,code]>0):                      # price can't be zero
        checking=frame[frame.index>date].head(keep_time)
        ans=((checking.High/frame.at[date,code]).max())
        when_sell=((checking.High/frame.at[date,code]).idxmax())
        return (checking.High/frame.at[date,code]).max() >= thres,ans,when_sell
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
        if dates_dict[x][0] <= min_date:       # dont open all files
            data = open_txt(x)
            if data[data.index>current_date].head(240).Low.min() <= total_money:       # find the min date at four months
                ans,res,when_sell=worth_buy(data,data[data.index>current_date].head(240).Low.idxmin(),'Low',thres=threl) # check worth
                if ans:
                    worthing.append([data[data.index>current_date].head(240).Low.idxmin(),x,res,when_sell])
                    if min_date_sell <= current_date:
                        min_date_sell = when_sell
                    if min_date_sell > when_sell:
                        min_date_sell = when_sell

    return sorted(worthing, key=itemgetter(2),reverse=True)



initialize()
data = open_txt(current_name)
indexing_list = data.index.values.tolist()      # in this way i can control dates

buy(current_name,current_date,'Low','1965-09-27')
current_date = '1965-09-27'
for key in list(purchased.keys()):
    if worth_sell(key):
        sell(current_date,key,'High')
while current_date <= '2017-10-11':
    if(total_money>1000000):
        founded=find_something(threl=500)
    elif(total_money>500000):
        founded=find_something(threl=200)
    elif(total_money>250000):
        founded=find_something(threl=100)
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
