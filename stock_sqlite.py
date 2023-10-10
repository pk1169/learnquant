import pandas as pd
import os
from sqlalchemy import create_engine

def connect_db(db):
    # "数据库类型+数据库驱动://数据库用户名:数据库密码@IP地址:端口号/数据库?编码"
    # echo=True echo用来设置SQLAlchemy日志，设置为True时，可以看见所有数据库的操作记录。
    engine = create_engine('sqlite:///{}.db'.format(db))
    return engine

def create_stock(tscode, db, code):
    # 读取本地CSV文件
    path = os.path.join(os.path.join(os.getcwd(), "datas"), tscode + ".csv")
    df = pd.read_csv(path)
    engine = connect_db(db)
    # name='stock_{}'.format(code)数据库表名，全部小写否则会报错,
    # if_exists='replace' 如果表存在，删除表，重建并插入给定的 DataFrame
    df.to_sql(name='stock_{}'.format(code), con=engine, index=False, if_exists='replace')
    
def read_csv(code):
    path = os.path.join(os.path.join(os.getcwd(), "datas"), code + ".csv")
    df = pd.read_csv(path)
    return df

def read_sql(code, db):
    sql = ' select * from {};'.format(code)
    engine = connect_db(db)
    # read_sql_query的两个参数: sql语句， 数据库连接
    df = pd.read_sql_query(sql, engine)
    return df

if __name__ == "__main__":
    #数据库名
    stockDB = 'tushare_data'
    stockList = 'stock_list'
    #创建stock_list股票列表的数据库表
    create_stock(tscode=stockList, db=stockDB, code=stockList)
    df = read_csv(stockList)
    data = list(df['ts_code'])
    
    #创建各股票表，如stock_600288、stock_002624等表
    for i in range(0, len(data)):
        codeStock = data[i].rstrip('.SZHBJ')
        create_stock(tscode=data[i], db=stockDB, code=codeStock)
        print(codeStock)