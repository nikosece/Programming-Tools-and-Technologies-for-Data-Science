from os import listdir
from os.path import isfile, join
import pandas as pd

total_money = 1
purchased = {}
current_date = '1970-01-01'
name = ''
thres = 150
keep_time = 120
def open_txt(name):
    return pd.read_csv('C:/Users/tzagk/Downloads/Stocks/'+name, header = 0, index_col=0)

def buy_total(frame,date,code):
    global total_money
    if frame.at[date,'Volume'] > 0 :
        max_amount = int(total_money // frame.at[date,code])
        max_allowed = int( frame.at[date,'Volume'] * 0.1)
        return max_amount if max_allowed > max_amount else max_allowed
    else:
        return 0

def buy(total,frame,date,name,code):
    global total_money
    global purchased
    if(total>0):
        print(date,'buy-'+code,name.split(sep='.')[0],total)
        total_money -= total * frame.at[date,code]
        if name in purchased:
            purchased[name] = purchased[name] + total
        else:
            purchased[name] = total


def sell_total(frame,date,name):
    global total_money
    global purchased
    max_amount = purchased[name]
    max_allowed = int( frame.at[date,'Volume'] * 0.1)
    return max_amount if max_allowed > max_amount else max_allowed

def sell(total,frame,date,name,code):
        global total_money
        global purchased
        print(date,'sell-'+code,name.split(sep='.')[0],total)
        total_money += total * frame.at[date,code]
        if purchased[name] == total:
            del purchased[name]
        else:
            purchased[name] -= total

def worth(frame,date):
    global thres
    global keep_time
    if(frame.at[date,'Low']>0):
        checking=frame[frame.index>date].head(keep_time)
        ans=((checking.High/frame.at[date,'Low']).max())
        return (checking.High/frame.at[date,'Low']).max() >= thres,ans
    else:
        return False,0


def initialize():
    file_names = [f for f in listdir('C:/Users/tzagk/Downloads/Stocks') if isfile(join('C:/Users/tzagk/Downloads/Stocks', f))]
    mylist = list()
    global current_date
    global name
    global data
    for x in file_names:        # find the min date that i can buy
        try:
            data = open_txt(x)
            current_date = data.index[0]
            ans,res=worth(data,current_date)
            if ans:
                print(x,'at',current_date,'is worth a lot',res)
        except:
            mylist.append('File '+x+' is empty')

    #

initialize()
