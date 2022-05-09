import re
from datetime import date, timedelta

import singer

from tap_appfigures.streams.base import AppFiguresBase
from tap_appfigures.utils import str_to_date, date_to_str, strings_to_floats
from tap_appfigures.utils import RequestError

LOGGER = singer.get_logger()


class RanksStream(AppFiguresBase):
    STREAM_NAME = 'ranks'
    KEY_PROPERTIES = ['product_id', 'country', 'category', 'date']

    def do_sync(self):
        start_date = str_to_date(self.bookmark_date)
        new_bookmark_date = start_date

        # README: BEGIN -> When it has more than 15 app_id, it has to be splited
        products = []
        final_list = lambda _list, x: [_list[i:i+x] for i in range(0, len(_list), x)]
        for p in final_list(list(filter(lambda x: str(x) not in self.sub_product_ids, self.product_ids)), 10):
            products.append(','.join(str(_id) for _id in p ))
        # README: END -> Created a small batch of app_ids

        while start_date.date() <= date.today():
            end_date = start_date + timedelta(days=28)
            for product_ids in products:
                uri = '/ranks/{}/daily/{}/{}/?tz=utc'.format(
                    product_ids,
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d')
                )
                data = self.client.make_request(uri).json()
                while data.get("status") == 400:
                    _id = re.findall('\d+', data['message'])
                    LOGGER.warning(f"This app ID doesnt exist in inapp for ranks: {_id}, removing and trying again")
                    uri = re.sub(f",{_id[0]}|{_id[0]},", "", uri)
                    if "," not in uri:
                        break
                    data = self.client.make_request(uri).json()
                if data.get("status") == 400:
                    continue
                rank_dates = data['dates']
                rank_data = data['data']

                with singer.metrics.Counter('record_count', {'endpoint': 'ranks'}) as counter:
                    for rank_entry in rank_data:
                        for i, rank_date in enumerate(rank_dates):
                            record_data = dict(
                                country=rank_entry['country'],
                                category=rank_entry['category'],
                                product_id=rank_entry['product_id'],
                                position=rank_entry['positions'][i],
                                delta=rank_entry['deltas'][i],
                                date=rank_date,
                            )

                            new_bookmark_date = max(new_bookmark_date, str_to_date(record_data['date']))
                            record_data = strings_to_floats(record_data)
                            singer.write_message(singer.RecordMessage(
                                stream=self.STREAM_NAME,
                                record=record_data,
                            ))
                            counter.increment()

                self.state = singer.write_bookmark(
                    self.state, self.STREAM_NAME, 'last_record', date_to_str(new_bookmark_date)
                )

            start_date = end_date
