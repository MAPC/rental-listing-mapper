# Rental Listing Mapper

## Overview

This repository pulls the table that is populated by the [scraper](https://github.com/mapc/rental-listing-scraper) module and maps the data into a format consumable by the [cleaner](https://github.com/mapc/rental-listing-cleaner)

In addition to creating a CSV in this format, this process also writes the mapped data to a `mapped` table in the `rental-listings-aggregator` database.

## Running the code

### Setup
First, you'll need to set up your environment variables. These can be set using a `.env` file in the root of this project. A template (`.env.template`) has been provided as an example. The production values are saved as a secure note in Dashlane.

Create a virtual environment if you haven't already: `python -m venv virtualenv_mapper`

Enter the virtual environment: `source virtualenv_mapper/bin/activate`

Then, install the requirements: `pip install -r requirements.txt`

With all of these steps completed, you can run the mapper: `python map.py`
