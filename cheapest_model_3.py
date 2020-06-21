'''Scrape Turo for future bargain rentals.'''
# pylint: disable=broad-except

import argparse
import datetime
import logging
import re
import string

import pandas as pd
import pgeocode
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from retry import retry

TURO_ROOT_URL = 'https://turo.com'

# Access DAYS by index, first element '' since we use isoweekday (shifted 1)
DAYS = ('', 'MONDAY', 'TUESDAY', 'WEDNESDAY',
        'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY')


def printable(full_string):
  return ''.join([x for x in full_string if x in set(string.printable)])


def day_of_week(weekday, date):
  return date + datetime.timedelta(days=weekday - date.isoweekday())


def dates_in_scope(weeks_ahead):
  today = datetime.date.today()
  if day_of_week(DAYS.index('FRIDAY'), today) < today:
    today += datetime.timedelta(weeks=1)
  return [today + datetime.timedelta(weeks=w) for w in range(weeks_ahead)]


@retry(delay=1, backoff=2, max_delay=8)
def get_turo_listings(start, end, zip_code, latitude, longitude, max_miles):
  '''Request Turo rental listings within start and end dates for a location.'''
  headers = {
      'Pragma': 'no-cache',
      'Accept-Encoding': 'gzip, deflate, br',
      'Accept-Language': 'en-US,es-US;q=0.8,es;',
      'Accept': '*/*',
      'Referer': TURO_ROOT_URL + '/search?',
      'Connection': 'keep-alive',
      'Cache-Control': 'no-cache',
  }

  params = (
      ('country', 'US'),
      ('endDate', end),
      ('endTime', '10:00'),
      ('itemsPerPage', '200'),
      ('location', str(zip_code)),
      ('locationType', 'ZIP'),
      ('maximumDistanceInMiles', str(max_miles)),
      ('Latitude', str(latitude)),
      ('Longitude', str(longitude)),
      ('sortType', 'RELEVANCE'),
      ('startDate', start),
      ('startTime', '10:00'),
      ('makes', 'Tesla'),
      ('models', 'Model 3'),
  )
  result = None
  url_to_fetch = requests.Request(
      'GET',
      TURO_ROOT_URL + '/api/search',
      params=params,
      headers=headers).prepare().url
  print(f'Requesting URL:\n\t{url_to_fetch}')
  try:
    result = requests.get(TURO_ROOT_URL + '/api/search',
                          headers=headers, params=params, timeout=15)
    if result is not None:
      print('Recieved response.\n')
      return result.json()
  except Exception as e:
    logging.warning('Exception while requesting listings: %s', e)


class Car():
  '''Represents an individual car rental.'''

  def __init__(self, car_json=None, vehicle_url=None):
    if car_json:
      self.date_accessed = datetime.date.today()
      self.instant_book = bool(car_json['instantBookDisplayed'])
      self.latitude = car_json['location']['latitude']
      self.longitude = car_json['location']['longitude']
      self.all_star_host = bool(car_json['owner']['allStarHost'])
      self.average_daily_price = float(car_json['rate']['averageDailyPrice'])
      rating = car_json['rating']
      self.rating = float(rating) if rating else None
      self.review_count = int(car_json['reviewCount'])
      self.renter_trips_taken = int(car_json['renterTripsTaken'])
      self.vehicle_trim = str(car_json['vehicle']['trim'])
      self.vehicle_year = int(car_json['vehicle']['year'])
      self.vehicle_url = str(TURO_ROOT_URL + car_json['vehicle']['url'])
    else:
      self.vehicle_url = vehicle_url

    self.performance_score = 0
    try:
      self.get_detailed_listing()
    except Exception as e:
      logging.error('Error requesting detailed listing info: %s', e)

  @retry(delay=1, backoff=2, max_delay=8)
  def get_detailed_listing(self):
    '''Request details not provided in initial result. (e.g. description)'''
    assert self.vehicle_url is not None
    page = requests.get(self.vehicle_url, timeout=15)
    soup = BeautifulSoup(page.content, 'html.parser')

    # Trim
    trims = []
    for label in soup.find_all('div', {'class': 'vehicleLabel'}):
      label = label.text
      trim = re.findall(r'(performance)|(standard)|(long)', label.lower())
      if trim:
        trims.append(set([x for x in trim[0] if x]).pop())
    self.trim = set(trims).pop() if trims else None
    self.performance_trim = self.trim == 'performance'
    if self.performance_trim:
      self.performance_score += 1

    # Description
    self.description = set([printable(x.text) for x in soup.find_all(
        'div', {'class': 'vehicleDetails-descriptionText'})]).pop()
    self.performance_description = any(
        [re.findall(r'performance', x.lower()) for x in self.description])
    if self.performance_description:
      self.performance_score += 1

    # Allowed mileage
    reservation_box = set([x.text for x in soup.find_all(
        'div', {'class': 'reservationBox'})]).pop()
    allowed_mileage = re.findall(
        r'Distance includedDay(\d+ mi|Unlimited)Week(\d+ mi|Unlimited)'
        r'Month(\d+ mi|Unlimited)', reservation_box)
    if allowed_mileage:
      self.day_miles, self.week_miles, self.month_miles = allowed_mileage[0]


@retry(delay=1, backoff=2, max_delay=8)
def create_dataset(client, dataset_name):
  dataset_id = "{}.{}".format(client.project, dataset_name)
  dataset = bigquery.Dataset(dataset_id)
  dataset.location = "US"
  dataset = client.create_dataset(dataset)
  logging.info("Created dataset %s", dataset_id)
  return dataset_id


@retry(delay=1, backoff=2, max_delay=8)
def create_or_assert_dataset(client, dataset_name):
  '''Create Biquery dataset if it does not already exist.'''
  datasets = list(client.list_datasets())
  project = client.project
  if datasets:
    if dataset_name not in [x.dataset_id for x in datasets]:
      create_dataset(client, dataset_name)
  else:
    logging.info("%s project does not contain any datasets.", project)
    create_dataset(client, dataset_name)
  return dataset_name


@retry(delay=1, backoff=2, max_delay=8)
def upload_table(client, table_id, df):
  # Create/replace current table
  job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
  job.result()
  logging.info('Successfully uploaded DataFrame to BigQuery: %s', table_id)
  return table_id


def main(request):
  # Zipcode validation and lat/long.
  try:
    location = pgeocode.Nominatim('us').query_postal_code(request.zip_code)
  except Exception as e:
    logging.error('Not a valid zip code %s', e)
    return

  uploaded_tables = []
  # Run for number of future weekends specified
  for date in dates_in_scope(request.num_future_weekends):
    start = day_of_week(DAYS.index('FRIDAY'), date)
    end = day_of_week(DAYS.index('SUNDAY'), date)

    # Pull turo listings according to flags
    print(f'Getting listings for weekend of {start}.')
    listings = get_turo_listings(start=start.strftime('%m/%d/%Y'),
                                 end=end.strftime('%m/%d/%Y'),
                                 zip_code=location.postal_code,
                                 latitude=location.latitude,
                                 longitude=location.longitude,
                                 max_miles=request.max_miles)['list']
    if not listings:
      logging.error('Could not retrieve Turo listings, '
                    'or Turo responded with an empty list.')
      return

    # Initialize DataFrame
    column_names = vars(Car(listings[0])).keys()
    df = pd.DataFrame(columns=column_names)

    num_listings = len(listings)
    for i, listing in enumerate(listings):
      current_listing = i + 1
      if current_listing % 5 == 0:
        print(f'Processed {current_listing} listings of {num_listings}.')
      try:
        rental = Car(listing)
      except Exception as e:
        logging.warning('Error storing listing: %s', e)
      row = dict(vars(rental))
      row['weekend'] = start
      df = df.append(row, ignore_index=True)

    print(df.drop(['description', 'vehicle_url'], axis=1).head().to_string())

    # Upload to BigQuery
    client = bigquery.Client()
    project = client.project
    dataset_name = create_or_assert_dataset(client, str(location.postal_code))
    table_name = start.strftime('%m_%d_%Y')
    table_id = f'{project}.{dataset_name}.{table_name}'

    uploaded_tables.append(upload_table(client, table_id, df))

  print('Successfully uploaded:\n - {}'.format('\n - '.join(uploaded_tables)))


if __name__ == '__main__':
  # Parse args
  parser = argparse.ArgumentParser(description='TuroBot')
  parser.add_argument("-v", "--verbose",
                      help="increase output verbosity", action="store_true")
  parser.add_argument('--num_future_weekends',
                      required=True, type=int, help='Weeks ahead')
  parser.add_argument('--zip_code', required=True, type=str, help='Zip code')
  parser.add_argument('--max_miles', required=False, type=int,
                      default=20, help='Maximum search area, in miles.')
  args = parser.parse_args()

  # Initialize logging
  if args.verbose:
    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info("Verbose output.")
  else:
    logging.basicConfig(format='%(levelname)s: %(message)s')

  main(args)
