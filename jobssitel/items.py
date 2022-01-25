# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobssitelItem(scrapy.Item):
    # define the fields for your item here like:
    job_url = scrapy.Field()
    job_title = scrapy.Field()
    job_location = scrapy.Field()
    job_posted_date = scrapy.Field()
    job_req_id = scrapy.Field()
    job_description = scrapy.Field()
    pass
