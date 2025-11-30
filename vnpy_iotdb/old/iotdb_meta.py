from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

'''
iotdb 节点定义
'''

# overview
OVERVIEW_COLUMN = [
    "count",
    "start",
    "end",
]

OVERVIEW_TYPE = [
    TSDataType.INT64,
    TSDataType.TIMESTAMP,
    TSDataType.TIMESTAMP,
]

OVERVIEW_ENCODING = [
    TSEncoding.PLAIN,
    TSEncoding.PLAIN,
    TSEncoding.PLAIN,
]

OVERVIEW_COMP = [
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
]


# bar
BAR_COLUMN = [
    "volume",
    "turnover",
    "open_interest",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
]

BAR_TYPE = [
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
]

BAR_ENCODING = [
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
]

BAR_COMP = [
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
]


# tick
TICK_COLUMN = [
    "volume",
    "turnover",
    "open_interest",
    "last_price",
    "last_volume",
    "limit_up",
    "limit_down",
    "open_price",
    "high_price",
    "low_price",
    "pre_close",
    "bid_price_1",
    "bid_price_2",
    "bid_price_3",
    "bid_price_4",
    "bid_price_5",
    "ask_price_1",
    "ask_price_2",
    "ask_price_3",
    "ask_price_4",
    "ask_price_5",
    "bid_volume_1",
    "bid_volume_2",
    "bid_volume_3",
    "bid_volume_4",
    "bid_volume_5",
    "ask_volume_1",
    "ask_volume_2",
    "ask_volume_3",
    "ask_volume_4",
    "ask_volume_5",
    "localtime",
]

TICK_TYPE = [
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
    TSDataType.TIMESTAMP,
]

TICK_ENCODING = [
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
    TSEncoding.TS_2DIFF,
]

TICK_COMP = [
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
    Compressor.LZ4,
]


# dr

DR_COLUMN = [
    "ratio",
    "diff",
]

DR_TYPE = [
    TSDataType.DOUBLE,
    TSDataType.DOUBLE,
]

DR_ENCODING = [
    TSEncoding.GORILLA,
    TSEncoding.GORILLA,
]

DR_COMP = [
    Compressor.LZ4,
    Compressor.LZ4,
]


assert len(OVERVIEW_COLUMN) == len(OVERVIEW_TYPE) == len(OVERVIEW_ENCODING) == len(OVERVIEW_COMP)
assert len(BAR_COLUMN) == len(BAR_TYPE) == len(BAR_ENCODING) == len(BAR_COMP)
assert len(TICK_COLUMN) == len(TICK_TYPE) == len(TICK_ENCODING) == len(TICK_COMP)
assert len(DR_COLUMN) == len(DR_TYPE) == len(DR_ENCODING) == len(DR_COMP)
