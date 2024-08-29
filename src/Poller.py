from pipeline.api import StockDataAPI
from pipeline.data_poller import DataPoller

api = StockDataAPI()

poller = DataPoller(
    macrotrends_api=api
)
poller.run()