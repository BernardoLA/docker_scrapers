import requests
import httpx
from httpx import ConnectError, TimeoutException, ReadTimeout
import random
from tenacity import retry, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_random, before_sleep
from urllib.parse import urljoin
from dataclasses import dataclass, asdict, fields
from selectolax.parser import HTMLParser
import re
import csv
import numpy as np
import time
import json
import pandas as pd
from pandas import concat
import datetime
import os


# Define the conditions for retrying based on exception types
def is_retryable_exception(exception):
    return isinstance(exception, (httpx.TimeoutException, httpx.ReadTimeout, httpx.ConnectError))

# # Define the conditions for retrying based on HTTP status codes
# def is_retryable_status_code(response):
#     return response.status_code in [500, 502, 503, 504]

@retry(
    retry=(retry_if_result(is_retryable_exception)),
    stop=stop_after_attempt(5),
    wait=wait_random(min=1, max=5),
)
def parse_api(url_api):
    try:
        r_json = httpx.get(url_api, timeout=60).json()
        return r_json  
    except Exception as e:
        print(F"Request Error in with API connection: {e}")
        raise e

# In this block of the code I work on extraction and cleaning of product information
@dataclass
class MyItem:
    ref: str
    name: str
    price: float
    tier1: str
    tier2: str
    tier3: str


def clean_prices(price):
    try:
        return ".".join(re.findall(r'\d+', price))
    except:
        return None


def extract_product_text(product, selector):
    try:
        return product.css_first(selector).text().strip()
    except AttributeError:
        return None

def make_product_item(product,html):
    new_item = MyItem(
        ref=extract_product_text(product, "span"),
        name=extract_product_text(product, "a.product-item-link"),
        price=clean_prices(extract_product_text(product, "span.price")),
        tier1=html.css("a.underline span")[2].text(),
        tier2=html.css("a.underline span")[3].text(),
        tier3=html.css("ol li.item.flex")[-1].css_first("span + span").text()
    )
    return new_item


@dataclass
class MyItemApi:
    product_color: str
    name: str
    ref: str
    stock: int
    total_incoming_stock: int


def get_detailed_stock(url_api, ref_number):
    parsed_json = parse_api(url_api)
    try:
        products = parsed_json['steps'][0]['options']
        for p in products:
            # I have to handle None values becuase I'm having Typer error therefore, the if statements
            api_item = MyItemApi(product_color=p['productCode'] + p['variantCode'],
                                name=p['name'],
                                ref=p['productCode'],
                                stock=p['stock']['quantity'] if p['stock'] else None,
                                total_incoming_stock=p['stock']['totalIncoming'] if p['stock'] else None,
                                )
            yield asdict(api_item)
    except (TypeError, AttributeError):
        print(f"There is no available deep stock info for product: {ref_number}")
        return None


@dataclass
class MyQuantDelApi:
    product_color: str
    arrival_date: datetime.datetime
    quantity: str
    occurrences: int


def clean_arrival_dates(date_string):
    string_to_date_obj = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
    formatted_date = string_to_date_obj.strftime('%d/%m/%Y')
    return formatted_date


def get_delivery_quantity(url_api, ref_number):
    parsed_json = parse_api(url_api)
    try:
        products = parsed_json['steps'][0]['options']
        for p in products:
            incoming_stock_info = p['stock']['incomingStocks'] if p['stock'] else None
            if incoming_stock_info:
                for (i, info) in enumerate(incoming_stock_info, start=1):
                    try:
                        arrival_date = clean_arrival_dates(info.get('expectedArrivalDate', None))
                    except (TypeError, AttributeError):
                        arrival_date = None
                    try:
                        quantity = info.get('quantity', None)
                    except (TypeError, AttributeError):
                        quantity = None

                    new_item = MyQuantDelApi(
                        product_color=p['productCode'] + p['variantCode'],
                        arrival_date=arrival_date,
                        quantity=quantity,
                        occurrences=int(i),
                    )
                    yield asdict(new_item)
            else:
                yield None
    except (TypeError, AttributeError):
        print(f"There is no available deep stock info for product: {ref_number}")
        return None


def clean_delivery_quantity_list(quantity_delivery_info):
    cleaned_list = [s for s in quantity_delivery_info if s is not None]
    return cleaned_list


def create_df_without_dup(list_of_dict, subset=None):
    df = pd.DataFrame(list_of_dict)
    if subset is None:
        print(f"Please, insert a subset to remove duplicates from dataframe.")
    return df.drop_duplicates(subset=subset)


def reshape_delivery_quantity(quantity_delivery_info_cleaned):
    # Create dataframe without duplicates
    df_duplicates_removed = create_df_without_dup(quantity_delivery_info_cleaned, ["product_color", "occurrences"])
    # Transform df from long to wide format
    df_wide = pd.pivot(data=df_duplicates_removed, index='product_color', columns='occurrences',
                       values=['arrival_date', 'quantity'])
    # Make sure to remove all indexes and flatten out the columns
    df_wide.columns = [f"{col[0]}_{col[1]}" for col in df_wide.columns]
    # need to rest indexes otherwise cannot pull Merge Key column
    df_wide = df_wide.reset_index()
    return df_wide


def join_general_and_deep_info(pd_del_quant_wide, pd_stock_info):
    return pd.merge(pd_stock_info, pd_del_quant_wide, how='left', on='product_color')


def export_file(dataframe, file_name):
    df = dataframe
    time_stamp = time.strftime('%d-%m-%Y_%H-%M-%S')
    #return df.to_excel(file_name + time_stamp + ".xlsx", index=False)
    return df.to_excel(f"/opt/airflow/data/{file_name}{time_stamp}.xlsx", index=False)

def new_urls(start_url):
    urls = []
    r = httpx.get(start_url)
    html = HTMLParser(r.text)
    for h in html.css("div.h-full")[2:-3]:
        for n in h.css("li.level-1"):
            for m in n.css("li.level-2 a")[:-1]:
                urls.append(m.attributes["href"])
    return urls               
    
def main():
    pages = range(1, 999)  # set to 100 because can break page when not found
    products_list = [] 
    stock_detailed_info = []
    quantity_delivery_info_cleaned = []
    quantity_delivery_info = []
    start = time.time()

    # Start Scraping
    url = "https://www.givingeurope.com/nl/nl/"
    urls = new_urls(start_url=url)
    nb_urls = len(urls)
    for url in urls:
        print(f"There are still {nb_urls} urls to be scraped.")
        # Loop over each product category and start in page 1 until max page
        for page in pages:
            new_url = url + f"?p={page}" + "&product_list_limit=48"
            #response = parse_url(new_url, header_list)
            response = httpx.get(new_url)
            html = HTMLParser(response.text)
            print(f"This is page number {page}.")
            # Loop over all products in a page 
            products = [div for div in html.css("ul[role='list'] > div")]
            for product in products:
                # Append to CSV - Product Ref, Name and Price all information
                products_list.append(asdict(make_product_item(product,html)))

                # Access API to get detailed stock and other info
                ref_number = make_product_item(product,html).ref
                url_api = "https://components.givingeurope.com/api/v1/products/" + ref_number + "/configurator?locale=nl_NL&layout=wholesale"

                # Get 1st level of Stock Information - Available Stock 
                stock_gen_info = get_detailed_stock(url_api, ref_number)
                [stock_detailed_info.append(s) for s in stock_gen_info]

                # Get deep level of Stock information - Incoming Stock and Incoming Dates     
                del_quant_info = get_delivery_quantity(url_api, ref_number)
                [quantity_delivery_info.append(qd) for qd in del_quant_info]

                # We need to remove the None values to later create the pandas dataframe from these dictionaries
                quantity_delivery_info_cleaned = clean_delivery_quantity_list(quantity_delivery_info)

            next_page = extract_product_text(html, "li.item.pages-item-next a span span")
            if next_page != "Volgende":  # change to Next once back
                break
            time.sleep(1)

        # urls count    
        nb_urls -= 1
        # Transform general data in df and remove duplicates
    general_stock_info = create_df_without_dup(products_list, subset=["ref", "price"])

    # Export Ref, Name, Price information
    #     general_stock_info.to_excel("impression_general_information_10-05-2024.xlsx", index=False)
    export_file(dataframe=general_stock_info, file_name="impression_general_information_")

    # Transform deep stock information, reshape data, and join tables.
    pd_del_quant_wide_unique = reshape_delivery_quantity(quantity_delivery_info_cleaned)
    pd_stock_info = create_df_without_dup(stock_detailed_info, ["product_color"])
    merged_data = join_general_and_deep_info(pd_del_quant_wide_unique, pd_stock_info)

    # Export data 
    #     merged_data.to_excel("impression_deep_stock_information_10-05-2024.xlsx", index=False)
    export_file(dataframe=merged_data, file_name="impression_deep_stock_info_")

    end = time.time()
    elapsed_time = (end - start) / 60
    # print(merged_data)
    print(f'Done. It took {elapsed_time} minutes')

if __name__ == "__main__":
    main()