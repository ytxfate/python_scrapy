from scrapy import cmdline

cmdline.execute('scrapy crawl weather_root'.split())


# from multiprocessing import Process
# from apscheduler.schedulers.background import BackgroundScheduler
# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings

# from weather_back.spiders.weather_spider import WeatherSpider

# settings = get_project_settings()

# process = CrawlerProcess()

# def start_crawl():
#     process = CrawlerProcess(settings)
#     process.crawl(WeatherSpider)
#     process.start()

# # scheduler = BackgroundScheduler()
# # scheduler.add_job(start_crawl_, 'cron', hour=16, minute = 4,second = 1)
# # try: 
# #     scheduler.start() 
# # except (KeyboardInterrupt, SystemExit): 
# #     scheduler.shutdown()

# if __name__ == "__main__":
#     start_crawl()