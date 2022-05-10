from tap_appfigures.streams.base import AppFiguresBase


class SubscriptionsStream(AppFiguresBase):
    STREAM_NAME = 'subscriptions'
    URI = '/reports/subscriptions?group_by=product,date,country&start_date={}&end_date={}&granularity=daily'
    KEY_PROPERTIES = ["country",'product_id', 'date']
    RESPONSE_LEVELS = 3
