'''
iotdb 数据库本体
'''

import numpy as np
import pandas as pd
from datetime import datetime
from contextlib import contextmanager
from iotdb.SessionPool import PoolConfig, SessionPool, Session
from iotdb.utils.NumpyTablet import NumpyTablet
from tqdm import tqdm
from concurrent.futures.thread import ThreadPoolExecutor
from vnpy.trader.constant import Exchange, Interval, ExtraInterval, Dividend
from vnpy.trader.object import BarData, TickData, DividendData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DividendOverview,
    DB_TZ,
    LOCAL_TZ,
    to_dbtz,
    from_dbtz,
)
from vnpy.trader.setting import SETTINGS


from .iotdb_meta import (
    TSDataType,
    OVERVIEW_COLUMN, OVERVIEW_TYPE,
    BAR_COLUMN, BAR_TYPE,
    TICK_COLUMN, TICK_TYPE,
    DR_COLUMN, DR_TYPE,
)

from .iotdb_utils import *


def list_split_by_size(self: list, size: int):
    self = list(self)
    g = []
    i = 0
    while True:
        s = self[i*size: (i+1)*size]
        i+=1
        if len(s) == size:
            g.append(s)
        elif len(s) > 0:
            g.append(s)
            break
        else:
            break
    return g


@contextmanager
def get_session(pool: SessionPool):
    session: Session = pool.get_session()
    yield session
    pool.put_back(session)


# -------------------------------------------------------------


class IoTDBDatabase(BaseDatabase):
    '''IoTDB数据库接口'''

    def __init__(self) -> None:
        '''构造函数'''
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.timezone: str = SETTINGS["database.timezone"]
        self.database: str = SETTINGS["database.database"]

        # 是否暂停 overview表更新，大量插入数据时，关闭可以提高插入性能
        self.pause_overview_update: bool = False
        # 如果暂停更新 ow表，则待更新的ow表值将设为 pause_overview_value，易于找到
        self.pause_overview_value: list[int] = (1, 2, 1)

        # 一些时间分区参数的定义
        _10year_ms = 10 * 365 * 24 * 60 * 60 * 1000
        _1year_ms = 365 * 24 * 60 * 60 * 1000
        _1month_ms = 1 * 30 * 24 * 60 * 60 * 1000
        _3month_ms = 3 * 30 * 24 * 60 * 60 * 1000
        _6month_ms = 6 * 30 * 24 * 60 * 60 * 1000

        # 设备模板配置
        self.temp_cfg = {
            'overview_temp':    [   OVERVIEW_COLUMN,    OVERVIEW_TYPE   ],
            'bar_temp':         [   BAR_COLUMN,         BAR_TYPE        ],
            'tick_temp':        [   TICK_COLUMN,        TICK_TYPE       ],
            'dr_temp':          [   DR_COLUMN,          DR_TYPE         ],
        }

        # 子储存组配置
        # 列定义
        # 对应的Interval，子数据库路径，使用的模板，时间分区间隔，概览存放到 overview 子数据库的路径
        self.sub_db_cfg = {
            'overview':             [   f'root.{self.database}.overview',   'overview_temp',_10year_ms, None                                    ],
            Interval.TICK:          [   f'root.{self.database}.tick',       'tick_temp',    _1month_ms, f'root.{self.database}.overview.tick'   ],
            Interval.MINUTE:        [   f'root.{self.database}.bar_1m',     'bar_temp',     _3month_ms, f'root.{self.database}.overview.bar_1m' ],
            Interval.MINUTE_5:      [   f'root.{self.database}.bar_5m',     'bar_temp',     _6month_ms, f'root.{self.database}.overview.bar_5m' ],
            Interval.DAILY:         [   f'root.{self.database}.bar_1d',     'bar_temp',     _1year_ms,  f'root.{self.database}.overview.bar_1d' ],
            ExtraInterval.Dividend: [   f'root.{self.database}.dr',         'dr_temp',      _10year_ms, f'root.{self.database}.overview.dr'     ],
        }

        # 连接数据库
        pool_config = PoolConfig(
            node_urls=[f"{self.host}:{self.port}"],
            user_name=self.user,
            password=self.password,
            time_zone=self.timezone,
            enable_compression=False,
            max_retry=3,
        )
        self.max_pool_size = 5          # 最多同时打开5个数据库连接
        wait_timeout_in_ms = 2*60*1000  # 最多等待2分钟
        self.session_pool = SessionPool(pool_config, self.max_pool_size, wait_timeout_in_ms)
        with get_session(self.session_pool) as session:
            session: Session

            # 仅用于测试
            # if _exist_db(xxx):
                # _delete_db(session, xxx)

            # 检查或创建模板
            for temp_name, [cols, types] in self.temp_cfg.items():
                if not exist_temp(session, temp_name):
                    create_temp(session, temp_name, cols, types)

            # 检查或创建子数据库，创建数据库后挂上模板
            for sub_db_name, [sub_db_path, temp_name, time_partition_interval, _] in self.sub_db_cfg.items():
                if not exist_db(session, sub_db_path):
                    create_db(session, sub_db_path, time_partition_interval)

                if not is_temp_mount_on_ts_path(session, temp_name, sub_db_path):
                    mount_temp_on_ts_path(session, temp_name, sub_db_path)

    # -------------------------------------------------------------
    # 辅助函数

    def get_ts_path(self, interval: Interval|ExtraInterval=None, symbol: str=None, exchange: Exchange=None):
        '''获取目标表的路径'''
        if interval in [Interval.DAILY, Interval.MINUTE, Interval.MINUTE_5, Interval.TICK, ExtraInterval.Dividend]:
            data_ts_path = f'{self.sub_db_cfg[interval][0]}.`{exchange.value}`.`{symbol}`'
            overview_ts_path = f'{self.sub_db_cfg[interval][3]}.`{exchange.value}`.`{symbol}`'
        else:
            raise AssertionError(f"Unknow interval: {interval}")
        return overview_ts_path, data_ts_path

    def get_find_ts_path(self, is_overview: bool, interval: Interval|ExtraInterval=None, symbol: str=None, exchange: Exchange=None):
        '''获取目标表的路径，查询用'''
        symbol_str = '*' if symbol is None else f'`{symbol}`'
        exchange_str = '*' if exchange is None else f'`{exchange.value}`'
        if interval in [Interval.DAILY, Interval.MINUTE, Interval.MINUTE_5, Interval.TICK, ExtraInterval.Dividend]:
            if is_overview:
                p = self.sub_db_cfg[interval][3]
            else:
                p = self.sub_db_cfg[interval][0]
            find_ts_path = f'{p}.{exchange_str}.{symbol_str}'
        else:
            raise AssertionError(f"Unknow interval: {interval}")
        return find_ts_path

    # def overview_ts_path_to_data_ts_path(self, overview_ts_path):
    #      = split_ts_path(overview_ts_path)
    #     return p

    def stat_ts_path(self, session, name, ts_path, start: datetime=None, end: datetime=None, to_datetime=False):
        '''获得目标表的统计信息'''
        sql = f'select count({name}) as count, MIN_TIME({name}) as start, MAX_TIME({name}) as end from {ts_path}'

        time_where_str = get_where_time_str(start, end)
        if time_where_str is not None:
            sql += f' where {time_where_str}'
        stat_df = session.execute_query_statement(sql).todf()
        if len(stat_df) == 0:
            return 0, None, None
        else:
            count, start, end = stat_df.iloc[0].to_list()
            if start is pd.NA:
                count, start, end = 0, None, None
            elif to_datetime:
                start = datetime.fromtimestamp(start/1000, DB_TZ)
                end = datetime.fromtimestamp(end/1000, DB_TZ)
            return count, start, end

    def _delete_data(self, session, overview_ts_path: str, data_ts_path: str, check_col_name: str, start: datetime = None, end: datetime = None) -> int:
        '''删除表里指定的数据，如果表空了，则同时删除表'''
        # 要删除的行数
        delete_rows, _, _ = self.stat_ts_path(session, check_col_name, data_ts_path, start, end)
        if delete_rows == 0:
            return 0

        # 删除数据
        where_time_str = get_where_time_str(start, end)
        sql = f'delete from {data_ts_path}.*'
        if where_time_str is not None:
            sql += f' where {where_time_str}'
        session.execute_non_query_statement(sql)

        # 删除后的行数
        count, start, end = self.stat_ts_path(session, check_col_name, data_ts_path, None, None)
        if count == 0:
            # 如果一行都没有了，直接删除该节点，同时删除汇总数据
            # delete_data_table(session, ts_path)
            delete_ts_path_on_temp(session, data_ts_path)
            delete_ts_path_on_temp(session, overview_ts_path)
        else:
            session.insert_record(overview_ts_path, 0, OVERVIEW_COLUMN, OVERVIEW_TYPE, [count, start, end])

        return delete_rows

    def _select_data(
        self,
        session,
        ts_path: str,
        col_names: list[str],
        start: datetime = None,
        end: datetime = None,
    ):
        '''查询表的数据'''
        sql = f"select {','.join(col_names)} from {ts_path}"
        where_time_str = get_where_time_str(start, end)
        if where_time_str is not None:
            sql += f' where {where_time_str}'
        sql += ' align by device'
        df = session.execute_query_statement(sql).todf()
        return df

    def _select_overview(
        self,
        session,
        find_ts_path: str,
    ):
        '''查询表汇总数据'''
        sql = f"select {','.join(OVERVIEW_COLUMN)} from {find_ts_path} where time == 0 align by device"
        df = session.execute_query_statement(sql).todf()
        return df

    # -------------------------------------------------------------
    def _rebuild_overview_run(self, overview_ts_path: str, data_ts_path: str, colname: str):
        try:
            with get_session(self.session_pool) as session:
                count, start, end = self.stat_ts_path(session, colname, data_ts_path)
                session.insert_record(overview_ts_path, 0, OVERVIEW_COLUMN, OVERVIEW_TYPE, [count, start, end])
        except Exception as e:
            print(f'Error in rebuild_overview: {str(e)}')

    def rebuild_overview(self):
        '''重建overview，只重建被 pause_update_overview 的'''
        need_refresh_ts_path = []
        with get_session(self.session_pool) as session:
            for interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.DAILY, Interval.TICK, ExtraInterval.Dividend]:
                if interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.DAILY]:
                    t = 'bar'
                elif interval == Interval.TICK:
                    t = 'tick'
                elif interval == ExtraInterval.Dividend:
                    t = 'dr'
                else:
                    raise AssertionError(f'Error! Unknow type {t}')
                find_ts_path = self.get_find_ts_path(True, interval=interval)
                # 专门搜索 self.pause_overview_value
                sql = f"select {','.join(OVERVIEW_COLUMN)} from {find_ts_path} where time == 0 and count == 1 align by device"
                df = session.execute_query_statement(sql).todf()
                for row in df.itertuples(index=False):
                    overview_ts_path = row.Device
                    count = row.count
                    start = int(row.start.to_pydatetime().timestamp() * 1000)
                    end = int(row.end.to_pydatetime().timestamp() * 1000)

                    if interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.DAILY]:
                        colname = BAR_COLUMN[0]
                    elif interval == Interval.TICK:
                        colname = TICK_COLUMN[0]
                    elif interval == ExtraInterval.Dividend:
                        colname = DR_COLUMN[0]
                    else:
                        raise AssertionError(f'Error! Unknow support interval {interval}')

                    if (count, start, end) == self.pause_overview_value:
                        exchange_str, symbol = split_ts_path(overview_ts_path)[-2:]
                        overview_ts_path, data_ts_path = self.get_ts_path(interval, symbol, Exchange(exchange_str))
                        params = [overview_ts_path, data_ts_path, colname]
                        need_refresh_ts_path.append(params)

        with ThreadPoolExecutor(self.max_pool_size, 'iotdb_rebuild_overview_thread') as pool:
            # 多线程重建 overview
            rs = []
            for params in need_refresh_ts_path:
                r = pool.submit(self._rebuild_overview_run, *params)
                rs.append(r)

            for r in tqdm(rs):
                r.result()

            pool.shutdown()

        # print(1)

    # -------------------------------------------------------------
    # bar

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        '''保存k线数据'''
        if len(bars) == 0:
            return True

        # 使用分块递归，避免提交过大，爆数据库内存
        max_g_size = 200000
        if len(bars) > max_g_size:
            # 如果超出大小，则执行自我递归操作
            r = False
            for g_bars in list_split_by_size(bars, max_g_size):
                r = self.save_bar_data(g_bars, False)
            return r
        else:
            # 否则正常执行
            pass
        #

        group_bar = {}

        # 如果 bar 有很多种，先分好类
        for bar in bars:
            bar.datetime = to_dbtz(bar.datetime)
            g_bars = group_bar.setdefault((bar.symbol, bar.exchange, bar.interval), [])
            g_bars.append(bar)

        with get_session(self.session_pool) as session:

            for key, bars in group_bar.items():
                symbol, exchange, interval = key
                overview_ts_path, data_ts_path = self.get_ts_path(interval, symbol, exchange)

                times = []
                values = [[] for _ in BAR_COLUMN]
                for bar in bars:
                    for idx, col in enumerate(BAR_COLUMN):
                        values[idx].append(getattr(bar, col))
                    times.append(to_iotdb_time(bar.datetime))

                times = np.asarray(times, TSDataType.TIMESTAMP.np_dtype())
                for idx, col in enumerate(values):
                    values[idx] = np.asarray(col, BAR_TYPE[idx].np_dtype())

                np_table = NumpyTablet(data_ts_path, BAR_COLUMN, BAR_TYPE, values, times)
                session.insert_aligned_tablet(np_table)

                if not self.pause_overview_update:
                    count, start, end = self.stat_ts_path(session, BAR_COLUMN[0], data_ts_path)
                else:
                    count, start, end = self.pause_overview_value
                session.insert_record(overview_ts_path, 0, OVERVIEW_COLUMN, OVERVIEW_TYPE, [count, start, end])

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
        dividend: Dividend=Dividend.NONE,
    ) -> list[BarData]:
        '''加载指定条件的K线数据

        Args:
            symbol: 交易品种代码
            exchange: 交易所
            interval: 时间间隔
            start: 开始时间
            end: 结束时间

        Returns:
            K线数据列表
        '''
        start = to_dbtz(start)
        end = to_dbtz(end)

        bars: list[BarData] = []
        with get_session(self.session_pool) as session:
            overview_ts_path, data_ts_path = self.get_ts_path(interval, symbol, exchange)
            df = self._select_data(session, data_ts_path, BAR_COLUMN, start, end)

            for row in df.itertuples(index=False):
                bar = BarData(
                    gateway_name='DB',
                    symbol=symbol,
                    exchange=exchange,
                    datetime=from_dbtz(from_iotdb_time(row.Time)),
                    interval=interval,
                    volume=row.volume,
                    turnover=row.turnover,
                    open_interest=row.open_interest,
                    open_price=row.open_price,
                    high_price=row.high_price,
                    low_price=row.low_price,
                    close_price=row.close_price,
                )
                bars.append(bar)

        self.apply_dividend('bar', bars, symbol, exchange, dividend)
        return bars

    def get_bar_overview(
        self,
        symbol: str = None,
        exchange: Exchange = None,
        interval: Interval = None
    ) -> list[BarOverview]:
        '''获取所有K线数据概览信息'''
        overviews: list[BarOverview] = []
        with get_session(self.session_pool) as session:

            if interval is None:
                interval_list = [Interval.MINUTE, Interval.MINUTE_5, Interval.DAILY]
            else:
                interval_list = [interval]

            for v in interval_list:
                find_ts_path = self.get_find_ts_path(True, v, symbol, exchange)
                df = self._select_overview(session, find_ts_path)
                for row in df.itertuples(index=False):
                    _, _, _, _, exchange_str, symbol_str = split_ts_path(row.Device)

                    overviews.append(
                        BarOverview(
                            symbol=symbol_str,
                            exchange=Exchange(exchange_str),
                            interval=Interval(v),
                            count=row.count,
                            start=from_dbtz(from_iotdb_time(row.start)),
                            end=from_dbtz(from_iotdb_time(row.end))
                        )
                    )

        return overviews

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        '''删除指定条件的K线数据'''
        start = to_dbtz(start)
        end = to_dbtz(end)

        overview_ts_path, data_ts_path = self.get_ts_path(interval, symbol, exchange)
        with get_session(self.session_pool) as session:
            delete_rows = self._delete_data(session, overview_ts_path, data_ts_path, BAR_COLUMN[0], start, end)
        return delete_rows

    # -------------------------------------------------------------
    # tick

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        '''保存tick数据'''
        if len(ticks) == 0:
            return True

        # 使用分块递归，避免提交过大，爆数据库内存
        max_g_size = 200000
        if len(ticks) > max_g_size:
            # 如果超出大小，则执行自我递归操作
            r = False
            for g_ticks in list_split_by_size(ticks, max_g_size):
                r = self.save_tick_data(g_ticks, False)
            return r
        else:
            # 否则正常执行
            pass
        #

        group_tick = {}

        # 如果 tick 有很多种，先分好类
        for tick in ticks:
            tick.datetime = to_dbtz(tick.datetime)
            tick.localtime = 0 if tick.localtime is None else to_dbtz(tick.localtime)
            g_ticks = group_tick.setdefault((tick.symbol, tick.exchange), [])
            g_ticks.append(tick)

        with get_session(self.session_pool) as session:

            for key, ticks in group_tick.items():
                symbol, exchange = key
                overview_ts_path, data_ts_path = self.get_ts_path(Interval.TICK, symbol, exchange)

                times = []
                values = [[] for _ in TICK_COLUMN]
                for tick in ticks:
                    for idx, (col, typ) in enumerate(zip(TICK_COLUMN, TICK_TYPE)):
                        if typ == TSDataType.TIMESTAMP:
                            values[idx].append(to_iotdb_time(getattr(tick, col)))
                        else:
                            values[idx].append(getattr(tick, col))
                    times.append(to_iotdb_time(tick.datetime))

                times = np.asarray(times, TSDataType.TIMESTAMP.np_dtype())
                for idx, col in enumerate(values):
                    values[idx] = np.asarray(col, TICK_TYPE[idx].np_dtype())

                np_table = NumpyTablet(data_ts_path, TICK_COLUMN, TICK_TYPE, values, times)
                session.insert_aligned_tablet(np_table)

                if not self.pause_overview_update:
                    count, start, end = self.stat_ts_path(session, TICK_COLUMN[0], data_ts_path)
                else:
                    count, start, end = self.pause_overview_value
                session.insert_record(overview_ts_path, 0, OVERVIEW_COLUMN, OVERVIEW_TYPE, [count, start, end])

        return True

    def load_tick_data(self, symbol: str, exchange: Exchange, start: datetime, end: datetime, dividend: Dividend=Dividend.NONE) -> list[TickData]:
        '''加载指定条件的Tick数据'''
        start = to_dbtz(start)
        end = to_dbtz(end)

        ticks = []
        with get_session(self.session_pool) as session:
            overview_ts_path, data_ts_path = self.get_ts_path(Interval.TICK, symbol, exchange)
            df = self._select_data(session, data_ts_path, TICK_COLUMN, start, end)

            for row in df.itertuples(index=False):
                tick = TickData(
                    gateway_name='DB',
                    symbol=symbol,
                    exchange=exchange,
                    datetime=from_dbtz(from_iotdb_time(row.Time)),
                    name='',
                    volume=row.volume,
                    turnover=row.turnover,
                    open_interest=row.open_interest,
                    last_price=row.last_price,
                    last_volume=row.last_volume,
                    limit_up=row.limit_up,
                    limit_down=row.limit_down,
                    open_price=row.open_price,
                    high_price=row.high_price,
                    low_price=row.low_price,
                    pre_close=row.pre_close,
                    bid_price_1=row.bid_price_1,
                    bid_price_2=row.bid_price_2,
                    bid_price_3=row.bid_price_3,
                    bid_price_4=row.bid_price_4,
                    bid_price_5=row.bid_price_5,
                    ask_price_1=row.ask_price_1,
                    ask_price_2=row.ask_price_2,
                    ask_price_3=row.ask_price_3,
                    ask_price_4=row.ask_price_4,
                    ask_price_5=row.ask_price_5,
                    bid_volume_1=row.bid_volume_1,
                    bid_volume_2=row.bid_volume_2,
                    bid_volume_3=row.bid_volume_3,
                    bid_volume_4=row.bid_volume_4,
                    bid_volume_5=row.bid_volume_5,
                    ask_volume_1=row.ask_volume_1,
                    ask_volume_2=row.ask_volume_2,
                    ask_volume_3=row.ask_volume_3,
                    ask_volume_4=row.ask_volume_4,
                    ask_volume_5=row.ask_volume_5,
                    localtime=from_dbtz(from_iotdb_time(row.localtime)),

                )
                ticks.append(tick)

        self.apply_dividend('tick', ticks, symbol, exchange, dividend)
        return ticks

    def get_tick_overview(self, symbol: str = None, exchange: Exchange = None, interval: Interval = None) -> list[TickOverview]:
        '''获取所有Tick数据概览信息'''
        overviews: list[TickOverview] = []
        with get_session(self.session_pool) as session:
            find_ts_path = self.get_find_ts_path(True, Interval.TICK, symbol, exchange)
            df = self._select_overview(session, find_ts_path)
            for row in df.itertuples(index=False):
                _, _, _, _, exchange_str, symbol_str = split_ts_path(row.Device)
                overviews.append(
                    TickOverview(
                        symbol=symbol_str,
                        exchange=Exchange(exchange_str),
                        count=row.count,
                        start=from_dbtz(from_iotdb_time(row.start)),
                        end=from_dbtz(from_iotdb_time(row.end))
                    )
                )

        return overviews

    def delete_tick_data(self, symbol: str, exchange: Exchange, start: datetime = None, end: datetime = None) -> int:
        '''删除指定条件的Tick数据'''
        start = to_dbtz(start)
        end = to_dbtz(end)

        overview_ts_path, data_ts_path = self.get_ts_path(Interval.TICK, symbol, exchange)
        with get_session(self.session_pool) as session:
            delete_rows = self._delete_data(session, overview_ts_path, data_ts_path, TICK_COLUMN[0], start, end)
        return delete_rows

    # -------------------------------------------------------------
    # dr

    def save_dividend_data(self, drs: list[DividendData]) -> bool:
        '''保存除权数据'''
        if len(drs) == 0:
            return False

        group_dr = {}

        # 如果 dr 有很多种，先分好类
        for dr in drs:
            dr.datetime = to_dbtz(dr.datetime)
            g_drs = group_dr.setdefault((dr.symbol, dr.exchange), [])
            g_drs.append(dr)

        with get_session(self.session_pool) as session:

            for key, drs in group_dr.items():
                symbol, exchange = key
                overview_ts_path, data_ts_path = self.get_ts_path(ExtraInterval.Dividend, symbol, exchange)

                times = []
                values = [[] for _ in DR_COLUMN]
                for dr in drs:
                    for idx, col in enumerate(DR_COLUMN):
                        values[idx].append(getattr(dr, col))
                    times.append(to_iotdb_time(dr.datetime))

                times = np.asarray(times, TSDataType.TIMESTAMP.np_dtype())
                for idx, col in enumerate(values):
                    values[idx] = np.asarray(col, DR_TYPE[idx].np_dtype())

                np_table = NumpyTablet(data_ts_path, DR_COLUMN, DR_TYPE, values, times)
                session.insert_aligned_tablet(np_table)

                if not self.pause_overview_update:
                    count, start, end = self.stat_ts_path(session, DR_COLUMN[0], data_ts_path)
                else:
                    count, start, end = self.pause_overview_value
                session.insert_record(overview_ts_path, 0, OVERVIEW_COLUMN, OVERVIEW_TYPE, [count, start, end])

        return True

    def load_dividend_data(self, symbol: str, exchange: Exchange, start: datetime, end: datetime) -> list[DividendData]:
        '''"查询数据库中的复权汇总信息'''
        start = to_dbtz(start)
        end = to_dbtz(end)

        drs: list[DividendData] = []
        with get_session(self.session_pool) as session:
            overview_ts_path, data_ts_path = self.get_ts_path(ExtraInterval.Dividend, symbol, exchange)
            df = self._select_data(session, data_ts_path, DR_COLUMN, start, end)

            for row in df.itertuples(index=False):
                dr = DividendData(
                    gateway_name='DB',
                    symbol=symbol,
                    exchange=exchange,
                    datetime=from_dbtz(from_iotdb_time(row.Time)),
                    ratio=row.ratio,
                    diff=row.diff,
                )
                drs.append(dr)
        return drs

    def get_dividend_overview(self, symbol: str=None, exchange: Exchange=None) -> list[DividendOverview]:
        '''"查询数据库中的除权汇总信息'''
        overviews: list[DividendOverview] = []
        with get_session(self.session_pool) as session:
            find_ts_path = self.get_find_ts_path(True, ExtraInterval.Dividend, symbol, exchange)
            df = self._select_overview(session, find_ts_path)
            for row in df.itertuples(index=False):
                _, _, _, _, exchange_str, symbol_str = split_ts_path(row.Device)
                overviews.append(
                    DividendOverview(
                        symbol=symbol_str,
                        exchange=Exchange(exchange_str),
                        count=row.count,
                        start=from_dbtz(from_iotdb_time(row.start)),
                        end=from_dbtz(from_iotdb_time(row.end))
                    )
                )

        return overviews

    def delete_dividend_data(self, symbol: str, exchange: Exchange, start: datetime=None, end: datetime=None) -> int:
        '''删除除权数据'''
        start = to_dbtz(start)
        end = to_dbtz(end)

        overview_ts_path, data_ts_path = self.get_ts_path(ExtraInterval.Dividend, symbol, exchange)
        with get_session(self.session_pool) as session:
            delete_rows = self._delete_data(session, overview_ts_path, data_ts_path, DR_COLUMN[0], start, end)
        return delete_rows

    # -------------------------------------------------------------
    #
