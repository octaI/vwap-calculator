# VWAP Calculator

## Description

This project is a simple proof of concept and usage of Websockets to consume data from the Coinbase PRO api to calculate [VWAP](https://en.wikipedia.org/wiki/Volume-weighted_average_price)
It also utilizes the multiprocessing module to achieve parallel processing.

The outputs are stored in txt files with names that identify the pair the vwap values correspond to, as well as the date.

It currently supports three pairs: `ETH-USD`, `BTC-USD` , `ETH-BTC` but it is easily extensible to add more.

## Installation

1. Create a venv. `python -m venv venv`
2. Source it. `source venv/bin/activate`
3. Install requirements via pip. `pip install -r requirements.txt`
4. Execute it. `python vwap_module.py`
