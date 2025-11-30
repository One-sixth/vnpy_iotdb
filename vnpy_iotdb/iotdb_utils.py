'''
iotdb 的一些包装好的工具，便于函数调用
'''

import csv
import pandas as pd
from datetime import datetime
from iotdb.SessionPool import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

# ----------------------------------------------------------------------
# 路径处理

class _IOTDB_PATH_Dialect(csv.Dialect):
    '''
    使用csv的解析器，便于分解路径
    '''
    delimiter = '.'                 # 字段分隔符
    doublequote = True              # 是否双写引号
    escapechar = None               # 转义字符
    lineterminator = '\n'           # 行终止符
    quotechar = '`'                 # 引号字符
    quoting = csv.QUOTE_ALL         # 引号模式
    skipinitialspace = False        # 是否跳过分隔符后的空格
    strict = True                   # 是否跳过分隔符后的空格


def split_ts_path(p: str) -> list[str]:
    # 正确分割 iotdb 的路径，并返回一个字符串列表
    return list(csv.reader([p], dialect=_IOTDB_PATH_Dialect))[0]

# ----------------------------------------------------------------------
# 时间处理

def to_iotdb_time(t: datetime|int) -> int:
    # 把datatime转换为iotdb需要的时间格式（毫秒）
    # 如果已经是int则保留原样
    if isinstance(t, int):
        return t
    else:
        return int(t.timestamp() * 1000)


def from_iotdb_time(t: int|pd.Timestamp) -> datetime:
    # iotdb读出来的time整数转换为 datatime
    # 如果是 pd.Timestamp 也转换为 datetime
    if isinstance(t, int):
        return datetime.fromtimestamp(t / 1000)
    else:
        return t.to_pydatetime()

# ----------------------------------------------------------------------
# 数据库操作

def exist_db(session: Session, db_name: str) -> bool:
    '''判断数据库是否存在'''
    sql = f'show databases {db_name}'
    df = session.execute_query_statement(sql).todf()
    return len(df) > 0

def create_db(session: Session, db_path, time_partition_interval: int= 30 * 24 * 60 * 60 * 1000):
    '''创建数据库，默认时间分区是30天'''
    sql = f'create database {db_path} with TIME_PARTITION_INTERVAL={time_partition_interval}'
    session.execute_non_query_statement(sql)

def delete_db(session: Session, db_path: str):
    '''删除数据库'''
    sql = f'delete database {db_path}'
    session.execute_non_query_statement(sql)

# ----------------------------------------------------------------------
# 设备树，模板操作

def make_create_template_sql(temp_name: str, aligned: bool, cols: list[str], types: list[TSDataType]) -> str:
    '''制作创建模板的sql语句'''
    s = ''
    for k, v in zip(cols, types, strict=True):
        s += f' {k} {v.name},'
    if len(s) != 0:
        # 去除逗号
        s = s[:-1]
    if aligned:
        o = f'create device template {temp_name} aligned ({s})'
    else:
        o = f'create device template {temp_name} ({s})'
    return o

def is_temp_mount_on_ts_path(session, temp_name, ts_path) -> bool:
    '''判断模板是否已挂载在指定路径上'''
    sql = f'show paths set device template {temp_name}'
    df = session.execute_query_statement(sql).todf()
    return ts_path in df['Paths'].to_list()

def exist_temp(session: Session, temp_name: str) -> bool:
    '''判断模板是否存在'''
    sql = f'show device templates'
    df = session.execute_query_statement(sql).todf()
    return temp_name in (df['TemplateName'].to_list())

def create_temp(session: Session, temp_name: str, cols: list[str], types: list[TSDataType]):
    '''创建设备模板'''
    sql = make_create_template_sql(temp_name, True, cols, types)
    session.execute_non_query_statement(sql)

def delete_temp(session: Session, temp_name: str):
    '''删除设备模板'''
    sql = f'drop device template {temp_name}'
    session.execute_non_query_statement(sql)

def delete_ts_path_on_temp(session: Session, ts_path: str):
    '''删除模板路径下的指定路径'''
    sql = f'delete timeseries of device template from {ts_path}'
    session.execute_non_query_statement(sql)

def mount_temp_on_ts_path(session: Session, temp_name: str, ts_path: str):
    '''挂载模板到指定路径'''
    sql = f'set device template {temp_name} to {ts_path}'
    session.execute_non_query_statement(sql)

# ----------------------------------------------------------------------
# SQL语句

def get_where_time_str(start: datetime=None, end: datetime=None):
    '''制作用于 where 的时间过滤表达式'''
    start_str = None if start is None else start.isoformat()
    end_str = None if end is None else end.isoformat()
    if start_str is not None and end_str is not None:
        where_str = f' time >= {start_str} and time <= {end_str}'
    elif start_str is not None:
        where_str = f' time >= {start_str}'
    elif end_str is not None:
        where_str = f' time <= {end_str}'
    else:
        where_str = None
    return where_str

# ----------------------------------------------------------------------
# 普通数据表操作，注意，不适用于 设备树模板模式

def exist_timeseries(session: Session, ts_path: str, data_cols: list[str]) -> bool:
    '''判断普通数据表是否存在，注意，这里仅支持对齐时间表'''
    return session.check_time_series_exists(f'{ts_path}.{data_cols[0]}')

def create_timeseries(
    session: Session, ts_path: str,
    data_cols: list[str], data_type: list[TSDataType], data_encoding: list[TSEncoding], data_comp: list[Compressor],
):
    '''创建普通数据表，不是设备树下的。自动跳过已经创建的表，通过检查data_cols第一个列是否已创建'''
    if not exist_timeseries(session, ts_path, data_cols):
        session.create_aligned_time_series(ts_path, data_cols, data_type, data_encoding, data_comp)

def delete_timeseries(session: Session, ts_path: str):
    '''删除普通数据表，不是设备树下的'''
    sql = f'delete timeseries {ts_path}.**'
    session.execute_non_query_statement(sql)

# ----------------------------------------------------------------------
