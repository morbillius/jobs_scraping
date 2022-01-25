import logging
import os
import re
import scrapy
from scrapy import Request
from jobssitel.spiders.job_item import JobItem


# for that each item wrote in csv a file, it is necessary to add pipelines-class in parameter ITEM_PIPELINES in the file settings.py
# I also added the "HTTPERROR_ALLOW_ALL = True" parameter in the setting.py, which allows scrapy to process requests with any response code (successful and unsuccessful)
# also the parameter "LOG_LEVEL = 'WARNING'" means to display only warnings and errors, it allows not to clutter output of logs in the console

class GenericSpider(scrapy.Spider):
    name = 'jobssitel'
    start_urls = ['https://jobs.sitel.com']
    start_page_url = 'https://jobs.sitel.com/go/Customer-Service-Jobs/7758100/'
    job_company = 'Sitel'
    scraped_items = 0
    total_items = 0
    items_urls = []
    page_urls = []
    active_file_name = None


    # main function for spider start
    def start_requests(self):
        print('Start spider:', self.start_urls[0])

        # here the file name with the results for each spider is initialized separately and used in pipelines
        self.active_file_name = self.get_active_file_name()

        # add the url of the page to the list, then check if it is already scraped and do not repeat
        self.page_urls.append(self.start_page_url)

        # request on the first page. there can also be a list of urls and go through them in a loop
        yield Request(self.start_page_url, dont_filter=True)
        return []


    def parse(self, response):
        url = response.url
        response_code = response.status

        # the server's response is being checked, all codes above 399 mean an error and it is better to log
        if int(response_code) > 399:
            self.log('[Jobs list] Bad request, code: ' + str(response_code) + ', url: ' + url, level=logging.ERROR)
            return

        items_per_page = 0

        # I get a list of items, here is a list of objects with which data will then be selected, so I do not call the .get () function
        table_rows = response.xpath('//table[@id="searchresults"]/tbody/tr')
        for table_row in table_rows:

            # to select from each object only its data, it is necessary to begin the selector with ./ instead of // that means a choice from all text of a site
            job_url = table_row.xpath('./td/span/a[@class="jobTitle-link"]/@href').get()

            # check if the url contains a domain
            if self.start_urls[0] not in job_url:
                job_url = self.start_urls[0] + job_url

            # check if such ulr has already been scraped to avoid duplicates
            if job_url not in self.items_urls:
                self.items_urls.append(job_url)

                # there is a request for the job page, the result will be processed in the method parse_job_page
                yield Request(job_url, self.parse_job_page, dont_filter=True)

                # counting the total number of items for logs
                self.total_items += 1
                items_per_page += 1

        # get the next page url
        next_page = response.xpath('//a[@class="paginationItemLast"]/@href').get()
        if next_page:
            # check if the url contains a domain
            if self.start_urls[0] not in next_page:
                next_page = self.start_urls[0] + next_page

            # check if this page has already been scribbled to avoid duplicates and loops
            if next_page not in self.page_urls:
                self.page_urls.append(next_page)

                # the result of the request will be processed by the same method parse()
                yield Request(next_page, dont_filter=True)

        print(' - Page parsed. Found items:', items_per_page, url)


    def parse_job_page(self, response):
        url = response.url
        response_code = response.status

        if int(response_code) > 399:
            self.log('[Job page] Bad request, code: ' + str(response_code) + ', url: ' + url, level=logging.ERROR)
            return

        # each resulting field is checked for None, because the strip() function (removes spaces on both sides) will cause an error if None
        job_title = response.xpath('//h1/span/text()').get()
        if job_title:
            job_title = job_title.strip()
            job_title = job_title.replace(u'\xa0', u' ').replace(u'\xe2\x80\x99', u"'")
        job_req_id = response.xpath('//span[@data-careersite-propertyid="customfield3"]/text()').get()
        if job_req_id:
            job_req_id = job_req_id.strip()
        job_location = response.xpath('//span[@class="jobGeoLocation"]/text()').get()
        if job_location:
            job_location = job_location.strip()
        job_posted_date = response.xpath('//meta[@itemprop="datePosted"]/@content').get()
        if job_posted_date:
            job_posted_date = job_posted_date.strip()

        # to get all the content from the item with HTML, you need to call the extract () function, which returns the list
        description = response.xpath('//span[@data-careersite-propertyid="description"]').extract()

        job_description = None
        job_description_clean = None
        if description and len(description) > 0:
            job_description = description[0]
            job_description = job_description.replace(u'\xa0', u' ').replace(u'\xe2\x80\x99', u"'")

            # here I try to get text without html, sometimes it is necessary, but there can be too many symbols of new line - \n
            job_description_clean = re.sub(' +', ' ', "\n".join(response.xpath('//span[@data-careersite-propertyid="description"]//text()').extract())).strip()
            job_description_clean = job_description_clean.replace(' ', ' ').replace('\r\n', '\n').replace('\n\n', '\n')

        # Scrapy documentation describes that you need to use an item object with the described fields (in the items.py file)
        # here is the code, how to use it:
        # scraper_item = JobssitelItem()
        # scraper_item["job_url"] = url
        # scraper_item["job_title"] = job_title
        # scraper_item["job_location"] = job_location
        # scraper_item["job_posted_date"] = job_posted_date
        # scraper_item["job_req_id"] = job_req_id
        # scraper_item["job_description"] = job_description

        # but you can also describe your object as a dictionary and the items.py file is not needed
        # this allows you to describe the fields in each spider separately (if a multi-spider project), and the fields in the CSV file will be in the same order
        # if this is not the case, you can simply delete the code below and uncomment the 121-127 lines

        # object in which new fields are processed based on job_title, job_location values
        item_object = JobItem(job_title, job_location)
        # item with one set of fields for all spiders
        item = item_object.init_item()

        item['job_url'] = url
        item['job_title'] = job_title
        item['job_location'] = job_location
        item['job_posted_date'] = job_posted_date
        item['job_req_id'] = job_req_id
        item['job_description'] = job_description
        item['job_company'] = self.job_company
        item['is_remote'] = item_object.get_is_remote()
        item['job_keywords'] = item_object.get_job_keywords()
        item['job_level'] = item_object.get_job_level()
        item['is_bilingual'] = item_object.get_is_bilingual()
        item['job_type'] = item_object.get_job_type()
        item['hourly_pay_rate'] = item_object.get_hourly_pay_rate()
        item['special_pay'] = item_object.get_special_pay()
        item['equipment_provided'] = item_object.get_equipment_provided()
        item['job_location_cleaned'] = item_object.get_cleaned()
        item['job_location_city_state'] = item_object.get_city_state()
        item['job_location_country'] = item_object.get_country()
        item['job_location_lat'] = item_object.get_lat()
        item['job_location_lon'] = item_object.get_lon()
        item['place_id'] = item_object.get_place_id()

        # here the item with data is returned, which will then be processed in the file pipelines.py
        yield item

        # self.scraped_items += 1
        # print('Item scraped', self.scraped_items, '/', self.total_items, url)


    def get_active_file_name(self):
        # here the path to the file for results is initialized, if there is no folder - creates it
        basedir = os.getcwd() + "\\jobssitel\\results\\"
        output_file_name = basedir + self.name + '_scraping_results.csv'
        if not os.path.exists(os.path.abspath(basedir)):
            os.makedirs(os.path.abspath(basedir))
        return output_file_name
