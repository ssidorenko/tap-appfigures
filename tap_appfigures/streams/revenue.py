from tap_appfigures.streams.base import AppFiguresBase


class RevenueStream(AppFiguresBase):
    STREAM_NAME = 'revenue'
    URI = '/reports/revenue?group_by=products,dates,country&start_date={}&end_date={}&granularity=daily'
    KEY_PROPERTIES = ["country", 'product_id', 'date']
    RESPONSE_LEVELS = 3
