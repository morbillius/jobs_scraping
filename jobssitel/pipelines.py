# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os
import sys
from datetime import datetime
import pymysql
import sqlalchemy
from google.cloud.sql.connector import connector
import jobssitel


class JobssitelPipeline:
    # here each item is written to the CSV file, which is returned from the parse() method
    def process_item(self, item, spider):
        try:
            # gets the path to the file from the variable of spider and checks if it exists to open for rewriting or create
            filepath = spider.active_file_name
            if os.path.exists(filepath):
                fmode = 'a'
            else:
                fmode = 'w'
            with open(filepath, fmode, newline='', encoding='utf8') as csv_file:
                fieldnames = item.keys()
                self.output_file_csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                self.has_headers = os.path.getsize(filepath) >= sys.getsizeof(fieldnames)
                # if the file is empty, the field headers are written
                if not self.has_headers:
                    self.output_file_csv_writer.writeheader()

                # the data of the item  are written
                self.output_file_csv_writer.writerow(dict(item))
        except Exception as e:
            print(repr(e))
        return item


class GoogleCloudSQLPipeline:
    existing_items = {}
    table_name = 'spider_jobs_data'
    conn = None
    new_items = 0
    updated_items = 0
    scraped_items = 0


    def __init__(self):
        # here the connection to base is created
        engine = self.init_connection_engine()
        self.conn = engine.connect()
        # checks if there is already a table exists, if not, creates it
        self.check_and_create_table()

        # function for debugging, clears the table
        # self.clear_table()
        # function for debugging, displays all data from the table
        # self.print_existing_items()

        # get all the data from the table and then uses it to set the job_last_seen parameter
        self.init_existing_items()
        print(' - GoogleCloudSQLPipeline initialized -')


    def process_item(self, item, spider):
        self.scraped_items += 1

        job_first_seen = self.get_date()
        job_req_id = item['job_req_id']
        job_url = item['job_url']
        job_title = item['job_title']
        job_location = item['job_location']
        job_posted_date = item['job_posted_date']
        job_description = item['job_description']
        job_company = item['job_company']
        is_remote = item['is_remote']
        job_keywords = item['job_keywords']
        job_level = item['job_level']
        is_bilingual = item['is_bilingual']
        job_type = item['job_type']
        hourly_pay_rate = item['hourly_pay_rate']
        special_pay = item['special_pay']
        equipment_provided = item['equipment_provided']
        job_location_cleaned = item['job_location_cleaned']
        job_location_city_state = item['job_location_city_state']
        job_location_country = item['job_location_country']
        job_location_lat = item['job_location_lat']
        job_location_lon = item['job_location_lon']
        place_id = item['place_id']

        # check whether there is already a scraped item in the database
        if job_req_id in self.existing_items.keys():
            row_id, job_last_seen = self.existing_items[job_req_id]
            # use check if you do not need to modify the date job_last_seen each time
            # if not job_last_seen:
            print(self.scraped_items, '/', spider.total_items, '[UPDATE]', job_req_id, job_url)
            job_last_seen = self.get_date()
            self.conn.execute(f"UPDATE {self.table_name} SET job_last_seen = '{job_last_seen}' WHERE id = '{row_id}'")
            self.updated_items += 1
        else:
            print(self.scraped_items, '/', spider.total_items, '[INSERT]', job_req_id, job_url)

            self.conn.execute(f"INSERT INTO {self.table_name} (job_req_id, job_url, job_title, job_location, job_posted_date, job_description, job_company, "
                              f"job_first_seen, is_remote, job_keywords, job_level, is_bilingual, job_type, hourly_pay_rate, special_pay, "
                              f"equipment_provided, job_location_cleaned, job_location_city_state, job_location_country, job_location_lat, job_location_lon, place_id) "
                              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                              (job_req_id, job_url, job_title, job_location, job_posted_date, job_description, job_company, job_first_seen, is_remote, job_keywords,
                               job_level, is_bilingual, job_type, hourly_pay_rate, special_pay, equipment_provided, job_location_cleaned, job_location_city_state,
                               job_location_country, job_location_lat, job_location_lon, place_id))
            self.new_items += 1
        return item

    # function that starts at the start of each spider
    def open_spider(self, spider):
        print(' - open_spider - ', spider.name)

    # create a connection to the database
    def init_connection_engine(self) -> sqlalchemy.engine.Engine:
        def getconn() -> pymysql.connections.Connection:
            conn: pymysql.connections.Connection = connector.connect(
                jobssitel.settings.GCDB_instance_connection,
                "pymysql",
                user=jobssitel.settings.GCDB_user,
                password=jobssitel.settings.GCDB_password,
                db=jobssitel.settings.GCDB_database,
            )
            return conn

        engine = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=getconn,
        )
        return engine


    def check_and_create_table(self):
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            " ( "
                "id int NOT NULL AUTO_INCREMENT, "
                "job_req_id TEXT NOT NULL, "
                "job_url TEXT NOT NULL, "
                "job_title TEXT NOT NULL, "
                "job_location TEXT NOT NULL, "
                "job_posted_date TEXT NOT NULL, "
                "job_description LONGTEXT NOT NULL, "
                "job_company TEXT NOT NULL, "
                "job_first_seen DATE NULL, "
                "job_last_seen DATE NULL, "
                "is_remote VARCHAR(4) NOT NULL, " 
                "job_keywords VARCHAR(255) NOT NULL, " 
                "job_level VARCHAR(255) NOT NULL, "
                "is_bilingual VARCHAR(4) NOT NULL, " 
                "job_type VARCHAR(255) NOT NULL, "
                "hourly_pay_rate VARCHAR(255) NOT NULL, " 
                "special_pay VARCHAR(255) NOT NULL, "
                "equipment_provided VARCHAR(4) NOT NULL, " 
                "job_location_cleaned TEXT NOT NULL, "
                "job_location_city_state VARCHAR(255) NOT NULL, "
                "job_location_country VARCHAR(255) NOT NULL, "
                "job_location_lat VARCHAR(255) NOT NULL, "
                "job_location_lon VARCHAR(255) NOT NULL, "
                "place_id TEXT NOT NULL, "
                "PRIMARY KEY (id) "
            ");"
        )
        print('Check and create table if not exists:', self.table_name)

    # print all data from the table
    def print_existing_items(self):
        select_stmt = sqlalchemy.text(f"SELECT * FROM {self.table_name}")
        rows = self.conn.execute(select_stmt).fetchall()
        for row in rows:
            print(row)

    # get all the data from the table and then uses it to set the job_last_seen parameter
    def init_existing_items(self):
        select_stmt = sqlalchemy.text(f"SELECT job_req_id, id, job_last_seen FROM {self.table_name}")
        rows = self.conn.execute(select_stmt).fetchall()
        for row in rows:
            job_req_id = row[0]
            row_id = row[1]
            job_last_seen = row[2]
            self.existing_items[job_req_id] = (row_id, job_last_seen)

        print('init_existing_items_id:', len(self.existing_items))

    # function that is called at the end of each spider's execution, print summarizes
    def close_spider(self, spider):
       print(' --- close_spider ---')
       print('New items:', self.new_items)
       print('Updated items:', self.updated_items)


    def clear_table(self):
        self.conn.execute(f"DELETE FROM {self.table_name}")


    def get_date(self):
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d")
        return current_time
