import googlemaps
from price_parser import Price


class JobItem():
    googlemaps_client_key = 'AIzaSyBLOtSjPX-zeuZd3I6l1NjrC-wPJSJ6KYQ'
    virtual_matches = ["virtual", "Virtual"]
    job_location_cleaned = ''
    job_location_lat = ''
    job_location_lng = ''
    job_location_place_id = ''
    job_location_city_state = ''
    job_location_country = ''
    is_remote = False


    def __init__(self, job_title, job_location):
        self.job_title = job_title.lower()
        self.job_location = job_location
        self.parse_job_location()


    def parse_job_location(self):
        if any(x in self.job_location.lower() for x in self.virtual_matches):
            self.job_location_cleaned = "Remote"
            self.is_remote = True
        else:
            self.is_remote = False
            gmaps = googlemaps.Client(key=self.googlemaps_client_key)
            r = gmaps.geocode(self.job_location)

            self.job_location_cleaned = r[0]['formatted_address']
            self.job_location_lat = r[0]['geometry']['location']['lat']
            self.job_location_lng = r[0]['geometry']['location']['lng']
            self.job_location_place_id = r[0]['place_id']

            city = ''
            state = ''
            country = ''
            address_components = r[0]['address_components']
            for addr in address_components:
                if 'locality' in addr['types']:
                    city = addr['long_name']
                if 'administrative_area_level_1' in addr['types']:
                    state = addr['long_name']
                if 'country' in addr['types']:
                    country = addr['short_name']

            self.job_location_city_state = city + ', ' + state
            self.job_location_country = country


    def init_item(self):
        scraper_item = {
            'job_url': '',
            'job_title': '',
            'job_location': '',
            'job_posted_date': '',
            'job_req_id': '',
            'job_description': '',
            'job_company': '',
            'is_remote': '',
            'job_keywords': '',
            'job_level': '',
            'is_bilingual': '',
            'job_type': '',
            'hourly_pay_rate': '',
            'special_pay': '',
            'equipment_provided': '',
            'job_location_cleaned': '',
            'job_location_city_state': '',
            'job_location_country': '',
            'job_location_lat': '',
            'job_location_lon': '',
            'place_id': '',
        }
        return scraper_item

    def get_is_remote(self):
        if self.is_remote:
            return 'Y'
        return 'N'

    def get_job_keywords(self):
        keys = []
        if 'insurance' in self.job_title:
            keys.append('Insurance Agent')
        if 'technical support' in self.job_title:
            keys.append('Technical Support')
        if 'customer service' in self.job_title:
            keys.append('Customer Service')
        if 'sales' in self.job_title:
            keys.append('Sales')
        if 'tax' in self.job_title:
            keys.append('Tax Representative')
        return ', '.join(keys)

    def get_job_level(self):
        if 'entry level' in self.job_title:
            return 'Entry Level'
        if 'junior' in self.job_title:
            return 'Junior'
        if 'manager' in self.job_title:
            return 'Manager'
        if 'supervisor' in self.job_title:
            return 'Supervisor'
        if 'executive' in self.job_title:
            return 'Executive'
        return ''

    def get_is_bilingual(self):
        if 'bilingual' in self.job_title:
            return 'Y'
        return 'N'

    def get_job_type(self):
        if 'temporary' in self.job_title:
            return 'Temporary'
        if 'part time' in self.job_title or 'part-time' in self.job_title:
            return 'Part Time'
        if 'full time' in self.job_title or 'full-time' in self.job_title:
            return 'Full Time'
        return ''

    def get_hourly_pay_rate(self):
        price = Price.fromstring(self.job_title)
        matches = ["hour", "hr", "hourly"]
        if any(x in self.job_title.lower() for x in matches):
            amount_float = price.amount_float
            if not amount_float:
                return ''
            if price.currency is None and self.job_location_country == 'US':
                return "$" + str(amount_float)
            else:
                currency = price.currency
                if not currency:
                    currency = ''
                return currency + str(amount_float)
        return ''

    def get_special_pay(self):
        if 'bonus' in self.job_title:
            return 'Bonus potential'
        return ''

    def get_equipment_provided(self):
        if 'equipment provided' in self.job_title:
            return 'Y'
        return 'N'

    def get_cleaned(self):
        return self.job_location_cleaned

    def get_city_state(self):
        return self.job_location_city_state

    def get_country(self):
        return self.job_location_country

    def get_lat(self):
        return self.job_location_lat

    def get_lon(self):
        return self.job_location_lng

    def get_place_id(self):
        return self.job_location_place_id