from tap_appfigures.streams.base import AppFiguresBase


class SalesStream(AppFiguresBase):
    STREAM_NAME = 'sales'
    URI = '/reports/sales/?group_by=products,dates,countries&start_date={}&end_date={}&granularity=daily'
    KEY_PROPERTIES = ["country",'product_id', 'date']
    RESPONSE_LEVELS = 3
