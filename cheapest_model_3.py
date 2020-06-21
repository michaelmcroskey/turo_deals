import argparse
import datetime
import logging
import pandas as pd
import pgeocode
import re
import requests
import string

from google.cloud import bigquery
from bs4 import BeautifulSoup

TURO_ROOT_URL = 'https://turo.com'

FRIDAY = 5
SATURDAY = 6
SUNDAY = 7


def printable(full_string):
  printable = set(string.printable)
  return ''.join([x for x in full_string if x in printable])


def dayOfWeek(weekday, date):
  shift = weekday - date.isoweekday()
  return(date + datetime.timedelta(days=shift))
  

def datesInScope(weeks_ahead):
  today = datetime.date.today()
  if dayOfWeek(FRIDAY, today) < today:
    today += datetime.timedelta(weeks=1)
  return [today + datetime.timedelta(weeks=w) for w in range(weeks_ahead)]

    
def getTuroListings(startDate, endDate, zip_code, latitude, longitude, maxMiles=20):
  headers = {
    'Pragma': 'no-cache',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,es-US;q=0.8,es;q=0.6,ru-BY;q=0.4,ru;q=0.2,en;q=0.2',
    'Accept': '*/*',
    'Referer': TURO_ROOT_URL + '/search?',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
  }

  params = (
      ('country', 'US'),
      ('endDate', endDate),
      ('endTime', '10:00'),
      ('itemsPerPage', '200'),
      ('location', str(zip_code)),
      ('locationType', 'ZIP'),
      ('maximumDistanceInMiles', str(maxMiles)),
      ('Latitude', str(latitude)),
      ('Longitude', str(longitude)),
      ('sortType', 'RELEVANCE'),
      ('startDate', startDate),
      ('startTime', '10:00'),
      ('makes', 'Tesla'),
      ('models', 'Model 3'),
  )
  result = None
  for i in range(3):
    url_to_fetch = requests.Request('GET', TURO_ROOT_URL + '/api/search', params=params, headers=headers).prepare().url
    print(f'Requesting URL:\n\t{url_to_fetch}')
    try:
      result = requests.get(TURO_ROOT_URL + '/api/search', headers=headers, params=params, timeout=15)
      if result != None:
        print('Recieved response.\n')
        break
    except Exception as e:
      logging.warn(e)
  return result.json()
  

class Car(dict):
  def __init__(self, car_json=None, vehicle_url=None):
    if car_json:
      self.date_accessed = datetime.date.today()
      self.instantBookDisplayed = bool(car_json['instantBookDisplayed'])
      self.latitude = car_json['location']['latitude']
      self.longitude = car_json['location']['longitude']
      self.allStarHost = bool(car_json['owner']['allStarHost'])
      self.averageDailyPrice = float(car_json['rate']['averageDailyPrice'])
      self.rating = float(car_json['rating'])
      self.reviewCount = int(car_json['reviewCount'])
      self.renterTripsTaken = int(car_json['renterTripsTaken'])
      self.vehicle_trim = car_json['vehicle']['trim']
      self.vehicle_year = int(car_json['vehicle']['year'])
      self.vehicle_url = TURO_ROOT_URL + car_json['vehicle']['url']
    else:
      self.vehicle_url = vehicle_url
      
    self.performance_score = 0
    self.getDetailedListing()
  
  def getDetailedListing(self):
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
    self.description = set([printable(x.text) for x in soup.find_all('div', {'class': 'vehicleDetails-descriptionText'})]).pop()
    self.performance_description = any([re.findall(r'performance', x.lower()) for x in self.description])
    if self.performance_description:
      self.performance_score += 1
    
    # Allowed mileage
    reservation_box = set([x.text for x in soup.find_all('div', {'class': 'reservationBox'})]).pop()
    allowed_mileage = re.findall(r'Distance includedDay(\d+ mi|Unlimited)Week(\d+ mi|Unlimited)Month(\d+ mi|Unlimited)', reservation_box)
    if allowed_mileage:
      self.day_miles, self.week_miles, self.month_miles = allowed_mileage[0]


def createDataset(client, dataset_name):
  dataset_id = "{}.{}".format(client.project, dataset_name)
  dataset = bigquery.Dataset(dataset_id)
  dataset.location = "US"
  dataset = client.create_dataset(dataset)
  logging.info("Created dataset {}".format(dataset_id))
  return dataset_id


def createOrAsserDataset(client, dataset_name):
  datasets = list(client.list_datasets())
  project = client.project
  if datasets:
    if dataset_name not in [x.dataset_id for x in datasets]:
      createDataset(client, dataset_name)
  else:
    logging.info("{} project does not contain any datasets.".format(project))
    createDataset(client, dataset_name)
  return dataset_name


def uploadTable(client, table_id, df):  
  # Create/replace current table
  job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
  job.result()
  logging.info(f'Successfully uploaded DataFrame to BigQuery: {table_id}')
  return table_id
    

def main(args):  
  # Zipcode validation and lat/long.
  try:
    location = pgeocode.Nominatim('us').query_postal_code(args.zip_code)
  except Exception as e:
    logging.error(f'Not a valid zip code {e}')
    return
    
  uploaded_tables = []
  # Run for number of future weekends specified
  for date in datesInScope(args.num_future_weekends):
    start = dayOfWeek(FRIDAY, date)
    end = dayOfWeek(SUNDAY, date)
    
    # Pull turo listings according to flags
    print(f'Getting listings for weekend of {start}.')
    listings = getTuroListings(startDate=start.strftime('%m/%d/%Y'),
                               endDate=end.strftime('%m/%d/%Y'),
                               zip_code=location.postal_code,
                               latitude=location.latitude,
                               longitude=location.longitude)['list']
    if not listings:
      logging.error('Could not retrieve Turo listings, or Turo responded with an empty list.')
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
        print(e, rental)
      row = dict(vars(rental))
      row['weekend'] = start
      df = df.append(row, ignore_index=True)

    print(df.drop(['description', 'vehicle_url'], axis=1).head().to_string())
    
    # Upload to BigQuery
    client = bigquery.Client()
    project = client.project
    dataset_name = createOrAsserDataset(client, str(args.zip_code))
    table_name = start.strftime('%m_%d_%Y')
    table_id = f'{project}.{dataset_name}.{table_name}'
    
    uploaded_tables.append(uploadTable(client, table_id, df))
  
  print('Successfully uploaded:\n - {}'.format('\n - '.join(uploaded_tables)))
  

if __name__ == '__main__':
  # Parse args
  parser = argparse.ArgumentParser(description='TuroBot')
  parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
  parser.add_argument('--num_future_weekends', required=True, type=int, help='Weeks ahead')
  parser.add_argument('--zip_code', required=True, type=int, help='Zip code')
  args = parser.parse_args()
  
  # Initialize logging
  if args.verbose:
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info("Verbose output.")
  else:
    logging.basicConfig(format='%(levelname)s: %(message)s')

  main(args)