from .products import ProductsStream
from .sales import SalesStream
from .revenue import RevenueStream
from .ratings import RatingsStream
from .subscriptions import SubscriptionsStream
from .usage import UsageStream
from .ranks import RanksStream

AVAILABLE_STREAMS = [
    ProductsStream,
    SalesStream,
    SubscriptionsStream,
    RevenueStream,
    RatingsStream,
    UsageStream,
    RanksStream
]
