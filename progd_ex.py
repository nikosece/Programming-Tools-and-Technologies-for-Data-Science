from os import listdir
from os.path import isfile, join
import pandas as pd

total_money = 1
purchased = {}              # quantity, buy price, date
min_date = '1970-01-01'
current_date = '1970-01-01'
current_name =''
name = ''
keep_time = 1465         # max time before sell,4 years
file_names =''
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
    global total_money
    global purchased
    global current_date
    frame = open_txt(name)
    total = buy_total(frame,date,'Low')
    if(total>0):
        print(date,'buy-'+code,name.split(sep='.')[0].upper(),total)
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
        global total_money
        global purchased
        global current_date
        frame = open_txt(name)
        total = sell_total(frame,date,name)
        print(date,'sell-'+code,name.split(sep='.')[0].upper(),total)
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
    global current_date,name,data,file_names,current_name
    file_names = [f for f in listdir('C:/Users/tzagk/Downloads/Stocks') if isfile(join('C:/Users/tzagk/Downloads/Stocks', f))]
    mylist = list()
    worthing=1
    for x in file_names:
        try:
            data = open_txt(x)
            if data.index[0] <= min_date:           #
                if data.head(120).Low.min() <=1:       # find the min date at four months
                    ans,res=worth_buy(data,data.head(120).Low.idxmin(),'Low',thres=1.2) # check worth
                    if ans:
                        if res > worthing:      # check best worth
                            worthing = res
                            current_date = data.head(120).Low.idxmin()  # set start date
                            current_name = x

        except:
            mylist.append('File '+x+' is empty')

    #

initialize()
buy(current_name,current_date,'Low')
for key in list(purchased.keys()):
    a,b,c=worth_sell(x,'High')
    if a:
        sell(c,x,'High')
