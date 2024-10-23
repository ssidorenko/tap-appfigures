"""
Stream base class
"""
import inspect
import os
from datetime import datetime, timedelta
import singer

from tap_appfigures.utils import str_to_date, strings_to_floats, RequestError

LOGGER = singer.get_logger()

def stream_details_from_catalog(catalog, stream_name):
    """
    Extract details for a single stream from the catalog
    """
    for stream_details in catalog.streams:
        if stream_details.tap_stream_id == stream_name:
            return stream_details
    return None


class AppFiguresBase:
    """
    Stream base class
    """
    BASE_URL = "https://api.appfigures.com/v2/"
    STREAM_NAME = ''
    URI = ''
    KEY_PROPERTIES = []
    RESPONSE_LEVELS = 2

    def __init__(self, client, state, catalog, sub_product_ids=[]):
        self.schema = None

        if catalog:
            stream_details = stream_details_from_catalog(catalog, self.STREAM_NAME)
            if stream_details:
                self.schema = stream_details.schema.to_dict()#['properties']
                self.key_properties = stream_details.key_properties

        if not self.schema:
            self.schema = singer.utils.load_json(
                os.path.normpath(
                    os.path.join(
                        self.get_class_path(),
                        '../schemas/{}.json'.format(self.STREAM_NAME))))
            self.key_properties = self.KEY_PROPERTIES
        LOGGER.warning(f"schema: {self.schema}")
        self.client = client
        self.state = state
        self.bookmark_date = singer.bookmarks.get_bookmark(
            state=state,
            tap_stream_id=self.STREAM_NAME,
            key='last_record'
        )
        if not self.bookmark_date:
            self.bookmark_date = client.start_date

        self.product_ids = []
        self.sub_product_ids = sub_product_ids

    def sync(self):
        """
        Perform sync action
        These steps are the same for all streams
        Differences between streams are implemented by overriding .do_sync() method
        """
        singer.write_schema(self.STREAM_NAME, self.schema, self.key_properties)
        self.do_sync()
        singer.write_state(self.state)

    @staticmethod
    def traverse_nested_dicts(dict_, levels):
        """
        The typical response from API requests is a series of nested dictionaries
        Flatten this and return as an iterator
        """
        for x in dict_.values():
            for y in x.values():
                # The dict_ is nested two or three levels deep
                if levels >= 3:
                    for z in y.values():
                        if levels == 4:
                            for a in z.values():
                                if a:
                                    yield a
                        else:
                            yield z
                else:
                    yield y

    def do_sync(self):
        """
        Main sync functionality
        Most of the streams use this
        A few of the streams work differently and override this method
        """
        start_date = (str_to_date(self.bookmark_date)-timedelta(14)).strftime('%Y-%m-%d')
        end_date = min((datetime.today() - timedelta(1)), str_to_date(self.bookmark_date) + timedelta(14)).strftime('%Y-%m-%d')

        try:
            response = self.client.make_request(self.URI.format(start_date, end_date))
        except RequestError as ex:
            LOGGER.error(ex)
            return
        LOGGER.error(response.json())
        records = []

        new_bookmark_date = self.bookmark_date
        LOGGER.info(f"Start bookmark: {new_bookmark_date}")
        with singer.metrics.Counter('record_count', {'endpoint': self.STREAM_NAME}) as counter:
            for entry in self.traverse_nested_dicts(response.json(), self.RESPONSE_LEVELS):
                # string to date
                # entry['date'] = str_to_date(entry['date'])
                # LOGGER.info(f"Entry content: {entry}")
                new_bookmark_date = max(new_bookmark_date, entry['date'])
                entry = strings_to_floats(entry)
                records.append(singer.RecordMessage(
                    stream=self.STREAM_NAME,
                    record=entry,
                ))
                if len(records) >= 2500:
                    for record in records:
                        singer.write_message(record)
                        records = []
            if records:
                for record in records:
                    singer.write_message(record)
            counter.increment()
        self.state = singer.write_bookmark(self.state, self.STREAM_NAME, 'last_record', new_bookmark_date)
        LOGGER.warning(f"state: {self.state}")

    def get_class_path(self):
        """
        The absolute path of the source file for this class
        """
        return os.path.dirname(inspect.getfile(self.__class__))

    # def generate_catalog(self):
    #     """
    #     Builds the catalog entry for this stream
    #     """
    #     return dict(
    #         tap_stream_id=self.STREAM_NAME,
    #         stream=self.STREAM_NAME,
    #         key_properties=self.key_properties,
    #         schema=self.schema,
    #         metadata={
    #             'selected': True,
    #             'schema-name': self.STREAM_NAME,
    #             'is_view': False,
    #         }
    #     )

    def generate_catalog(self):
        """
        Builds the catalog entry for this stream
        """
        metadata_list = []

        # Add top-level metadata entry with empty breadcrumb
        metadata_list.append({
            'metadata': {
                'selected': True,
                'schema-name': self.STREAM_NAME,
                'is_view': False,
                'inclusion': 'available'  # or 'selected' if you want it pre-selected
            },
            'breadcrumb': []
        })

        # Add metadata entries for each property in the schema
        # print(self.schema)
        for prop in self.schema['properties'].keys():
            if prop in self.key_properties:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata_list.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop]
            })

        return {
            'tap_stream_id': self.STREAM_NAME,
            'stream': self.STREAM_NAME,
            'key_properties': self.key_properties,
            'schema': self.schema,
            'metadata': metadata_list
        }
