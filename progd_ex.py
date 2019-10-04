from os import listdir
from os.path import isfile, join,getsize
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

def open_txt(name):
    return pd.read_csv('C:/Users/tzagk/Downloads/Stocks/'+name, header = 0, index_col=0)

def buy_total(frame,date,code):
    global total_money
    if frame.at[date,'Volume'] > 0 :            # Volume can't be zero
        max_amount = int(total_money // frame.at[date,code])
        max_allowed = int( frame.at[date,'Volume'] * 0.1)
        return max_amount if max_allowed > max_amount else max_allowed
    else:
        return 0

def buy(name,date,code):
    global total_money,purchased,current_date,transactions
    frame = open_txt(name)
    total = buy_total(frame,date,'Low')
    if(total>0):
        transactions.append(date,'buy-'+code,name.split(sep='.')[0].upper(),total)
        total_money -= total * frame.at[date,code]
        current_date = date
        if name in purchased:
            purchased[name] = [(purchased[name] + total),frame.at[date,code],date]
        else:
            purchased[name] = [total,frame.at[date,code],date]


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
        transactions.append(date,'sell-'+code,name.split(sep='.')[0].upper(),total)
        total_money += total * frame.at[date,code]
        current_date = date
        if purchased[name][0] == total:
            del purchased[name]
        else:
            purchased[name][0] -= total

def worth_buy(frame,date,code,thres=1.5):              #thelei ftiaksimo wste na pairnei sosto date, oxi current
    global keep_time
    if(frame.at[date,code]>0):                      # price can't be zero
        checking=frame[frame.index>date].head(keep_time)
        ans=((checking.High/frame.at[date,code]).max())
        return (checking.High/frame.at[date,code]).max() >= thres,ans
    else:
        return False,0

def worth_sell(name,code,thres=1.5):  # che if i should sell
    global keep_time
    global purchased
    frame = open_txt(name)
    date = purchased[name][2]
    if(frame.at[date,code]>0):                      # price can't be zero
        checking=frame[frame.index>date].head(keep_time)
        ans=((checking.High/purchased[name][1]).max())
        when = ((checking.High/purchased[name][1]).idxmax())
        return ((checking.High/purchased[name][1]).max()) >= thres,ans,when
    else:
        return False,0,''


def initialize():
    global current_date,name,data,file_names,current_name,dates_dict,mylist
    worthing=1
    for x in file_names:
        if getsize('C:/Users/tzagk/Downloads/Stocks/'+x)>0:
            data = open_txt(x)
            dates_dict[x] = data.index[0]               # store start_time
            mylist.append(x)
            if data.index[0] <= min_date:           #
                if data.head(120).Low.min() <=1:       # find the min date at four months
                    ans,res=worth_buy(data,data.head(120).Low.idxmin(),'Low',thres=1.2) # check worth
                    if ans:
                        if res > worthing:      # check best worth
                            worthing = res
                            current_date = data.head(120).Low.idxmin()  # set start date
                            current_name = x



def find_something():           # find an oportunity and return name and date
    global current_date,total_money,min_date,current_name,dates_dict,mylist
    worthing=1
    temp_date = current_date
    i=0
    for x in mylist:
        if dates_dict[x] <= min_date:       # dont open all files
            data = open_txt(x)
            if data.index[0] > current_date:           #ayto fainetai lathos
                if data.head(120).Low.min() <= total_money:       # find the min date at four months
                    ans,res=worth_buy(data,data.head(120).Low.idxmin(),'Low',thres=1.5) # check worth
                    if ans:
                        if res > worthing:      # check best worth
                            worthing = res
                            temp_date = data.head(120).Low.idxmin()  # set start date
                            current_name = x
    return current_name,temp_date
def chang_date():
    global min_date,current_date
    try:
        min_date = indexing_list[indexing_list.index(current_date)+1825]
    except:
        min_date = '2017-10-11'

initialize()
data = open_txt(current_name)
indexing_list = data.index.values.tolist()      # in this way i can control dates
buy(current_name,current_date,'Low')
for key in list(purchased.keys()):
    a,b,c=worth_sell(key,'High')
    if a:
        sell(c,key,'High')
while current_date <= '2017-10-11':
    who,when=find_something()
    buy(who,when,'Low')
    a,b,c = worth_sell(who,'High')
    if a:
        sell(c,who,'High')
    chang_date()
print(total_money)
print(transactions)
