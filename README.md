# turo_deals
 Find deals on Turo car rentals.

## Setup
```
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt

export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
```

## Usage
```
python3 cheapest_model_3.py \
 --zip_code 94040 \
 --num_future_weekends 3
```