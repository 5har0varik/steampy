import os
import re
import copy
import math
import struct
import json
import random
import datetime
import time
import http.client
from typing import List
from decimal import Decimal
from urllib.parse import urlparse, parse_qs

import requests
import aiohttp
import logging
import asyncio
from itertools import cycle
from tenacity import retry, stop_after_attempt, retry_if_result, retry_if_exception_type, wait_fixed
from bs4 import BeautifulSoup, Tag
from requests.structures import CaseInsensitiveDict

from steampy.models import GameOptions
from steampy.exceptions import ProxyConnectionError, LoginRequired


class ProxyCarousel:
    def __init__(self, json_filename, max_usage=15, cooldown_after_429=3600):
        self.proxy_list = []
        self.json_file = json_filename
        if os.path.exists(self.json_file):
            f = open(self.json_file, "r")
            self.proxy_data = json.loads(f.read())
            response = requests.get(self.proxy_data["Url"], headers=self.proxy_data["Headers"])
            print(response.json())
            for item in response.json()["results"]:
                # https='socks5://user:pass@host:port'
                if item['valid']:
                    # self.proxy_list.append({"https": "socks5://" + item['username'] + ':' + item['password'] + '@' +
                    #                                  item['proxy_address'] + ':' + str(item['port'])})
                    self.proxy_list.append("http://" + item['username'] + ':' + item['password'] + '@' +
                                                     item['proxy_address'] + ':' + str(item['port']))
        else:
            print("No json with proxy setup")
        self.sync_proxy_part = 0.1
        self.proxy_list_async = self.proxy_list[int(len(self.proxy_list) * self.sync_proxy_part): ]
        self.proxy_list_async_shuffled = self.proxy_list_async.copy()
        random.shuffle(self.proxy_list_async_shuffled)
        self.proxy_list_sync = self.proxy_list[: int(len(self.proxy_list) * self.sync_proxy_part)]
        self.max_usage = max_usage
        self.cooldown_after_429 = cooldown_after_429
        self.proxy_cycle = cycle(self.proxy_list_sync)
        self.proxy_usage_count = {proxy: 0 for proxy in self.proxy_list}
        self.ban_proxy_time = {proxy: 0 for proxy in self.proxy_list}
        self.current_proxy = next(self.proxy_cycle)

    def get_next_proxy(self, is_forced=False):
        # FIXME: we can get looped
        refresh_proxy_count = len(self.proxy_list_sync)
        while refresh_proxy_count > 0:
            print(refresh_proxy_count)
            # Get the next proxy in the cycle
            next_proxy = self.current_proxy
            if is_forced:
                self.proxy_usage_count[next_proxy] = 0
                self.ban_proxy_time[next_proxy] = datetime.datetime.now().timestamp() + self.cooldown_after_429
                next_proxy = next(self.proxy_cycle)

            # Check if the proxy can be used based on the usage count and cooldown time

            if (self.ban_proxy_time[next_proxy] - datetime.datetime.now().timestamp() < 0) and \
                    (self.proxy_usage_count[next_proxy] < self.max_usage):
                self.proxy_usage_count[next_proxy] += 1
                if self.proxy_usage_count[next_proxy] >= self.max_usage:
                    self.ban_proxy_time[next_proxy] = datetime.datetime.now().timestamp() + self.cooldown_after_429
                    self.proxy_usage_count[next_proxy] = 0
                return next_proxy
            else:
                # If the proxy has reached its usage limit, try the next one
                print(f"Proxy {next_proxy} reached usage limit. Trying the next one.")
                is_forced = True
                refresh_proxy_count -= 1
        print("Looped")
        return next_proxy

    def get_current_proxy(self):
        if self.current_proxy is None:
            self.update_current_proxy()
        return self.current_proxy

    def update_current_proxy(self, is_forced=False):
        self.current_proxy = self.get_next_proxy(is_forced)
        return self.current_proxy

    def get_random_async_proxy(self):
        """
        Return a proxy that is not currently banned or in cooldown.
        Reshuffle the list if needed and avoid banned proxies.
        """
        result = ""
        current_time = time.time()

        # Filter out proxies that are banned (cooldown not expired)
        available_proxies = [
            proxy for proxy in self.proxy_list_async_shuffled
            if self.ban_proxy_time.get(proxy, 0) <= current_time  # Use get() to avoid KeyError
        ]

        # If the available list is empty, refresh the shuffled list
        if len(available_proxies) == 0:
            # Reset the shuffled list with proxies that are not banned
            self.proxy_list_async_shuffled = [
                proxy for proxy in self.proxy_list_async if self.ban_proxy_time.get(proxy, 0) <= current_time
            ]
            if len(self.proxy_list_async_shuffled) == 0:
                raise Exception("No available proxies; all are banned or in cooldown.")
            random.shuffle(self.proxy_list_async_shuffled)

        # Pop a proxy from the shuffled list
        result = self.proxy_list_async_shuffled.pop()

        # Increment the usage count for the selected proxy
        self.proxy_usage_count[result] += 1

        return result


class SafeSession(requests.Session):
    def __init__(self, proxy_carousel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_carousel = proxy_carousel
        self.ban_time = 0
        self.cooldown_427 = 1800


    @staticmethod
    def return_last_value(retry_state):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data
        """return the result of the last call attempt"""
        response = MockResponse({"status_code": 404}, 404)
        try:
            retry_state.outcome.result()
        except Exception as e:
            code = int(e.args[0][:e.args[0].find(" ")])
            response = MockResponse({"status_code": code}, code)
        return response

    @staticmethod
    def is_false(value):
        """Return True if value is False"""
        return value is False

    @staticmethod
    def change_parameter(new_param):
        def _set_parameter(retry_state):
            retry_state.kwargs['retry_429'] = new_param
        return _set_parameter

    @retry(stop=stop_after_attempt(10),
           wait=wait_fixed(5),
           retry_error_callback=return_last_value,
           after=change_parameter(True),
           retry=(retry_if_result(is_false) |
                  retry_if_exception_type((json.JSONDecodeError,
                                           requests.exceptions.RequestException,
                                           requests.exceptions.ConnectionError,
                                           requests.exceptions.Timeout,
                                           requests.exceptions.HTTPError,
                                           http.client.HTTPException,
                                           http.client.RemoteDisconnected)))
           )
    def _safe_get_post(self, url, expect_json=True, is_get=True, use_proxy=False, retry_429=False, **kwargs):
        try:
            if self.ban_time - datetime.datetime.now().timestamp() > 0:
                retry_429 = True
            if use_proxy or retry_429:
                print(self.proxy_carousel.get_current_proxy())
                proxy = self.proxy_carousel.get_current_proxy()
                kwargs['proxies'] = {'http': proxy, 'https': proxy}

            response = self.get(url, **kwargs) if is_get else self.post(url, **kwargs)
            response.raise_for_status()  # Raises HTTPError for bad responses

            # Check if the response content is JSON if expected
            if expect_json:
                try:
                    json_content = response.json()
                    # If parsing as JSON is successful, return the JSON content
                    return response
                except json.JSONDecodeError:
                    # If parsing as JSON fails, raise an exception to trigger the retry
                    raise requests.exceptions.RequestException("Invalid JSON content")
            else:
                # If not expecting JSON, return the plain text content without retrying
                return response
        except requests.exceptions.RequestException as e:
            # Handle exceptions (e.g., ConnectionError, Timeout, HTTPError)
            if e.response is not None and e.response.status_code == 429:
                if not use_proxy:
                    print("Too many requests")
                    self.ban_time = datetime.datetime.now().timestamp() + self.cooldown_427
                    if retry_429:
                        self.proxy_carousel.update_current_proxy(True)
                else:
                    print("Too many requests with proxy. Change proxy")
                    self.proxy_carousel.update_current_proxy(True)
            elif e.response is not None and e.response.status_code == 403:
                return e.response
            if expect_json:
                print(f"Error during GET request or invalid JSON content: {e}")
            else:
                print(f"Error during GET request: {e}")
            raise  # Reraise the exception to trigger the retry

    def safe_post(self, url, expect_json=True, use_proxy=False, **kwargs):
        return self._safe_get_post(url, expect_json=expect_json, is_get=False, use_proxy=use_proxy, **kwargs)

    def safe_get(self, url, expect_json=True, use_proxy=False, **kwargs):
        return self._safe_get_post(url, expect_json=expect_json, is_get=True, use_proxy=use_proxy, **kwargs)


class AsyncSession():
    def __init__(self, timeout: float = 10, retries: int = 3, default_headers=None, proxy_carousel=None,
                 max_concurrency=100, *args, **kwargs):
        self.proxy_carousel = proxy_carousel
        self.timeout_ = aiohttp.ClientTimeout(total=timeout)
        self.retries = retries
        self.backoff_factor = 0.5
        self.default_headers = default_headers or {}
        self.semaphore = asyncio.Semaphore(max_concurrency)
        kwargs['timeout'] = self.timeout_
        self._session = None
        # super().__init__(headers=self.default_headers, *args, **kwargs)

    async def _get_session(self, *args, **kwargs):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(*args, **kwargs)

    async def close_aiohttp_session(self):
        """
        Asynchronous method to close aiohttp.ClientSession.
        """
        if self._session is not None:
            print("Closing aiohttp.ClientSession...")
            await self._session.close()
            self._session = None

    def close(self):
        """
        Synchronous method to close aiohttp.ClientSession using asyncio.
        """
        asyncio.run(self.close_aiohttp_session())

    async def _async_get_post(self, url, expect_json=True, method="GET", proxy="", **kwargs):
        async with self.semaphore:  # Limit concurrency
            await self._get_session()
            proxy = self.proxy_carousel.get_random_async_proxy()
            attempt = 0

            while attempt < self.retries:
                try:
                    async with self._session.request(method, url, proxy=proxy, **kwargs) as response:
                        response.raise_for_status()  # Raise an exception for HTTP errors

                        # Validate response content type and check for JSON if expected
                        content_type = response.headers.get('Content-Type', '')
                        if 'json' in content_type:
                            json_data = await response.json()
                            if not json_data:  # Ensure JSON is not empty
                                logging.warning("Received empty JSON response")
                                raise ValueError("Received empty JSON response")
                            if 'success' in json_data and json_data['success'] != 1:
                                logging.warning("JSON has success field but wasn't 1")
                                raise ValueError("JSON has success field but wasn't 1")
                            return json_data
                        else:
                            text_data = await response.text()
                            if not text_data:  # Ensure text response is not empty
                                raise ValueError("Received empty text response")
                            return text_data

                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        logging.warning("Too many requests, changing proxy.")
                        proxy = self.proxy_carousel.get_random_async_proxy()
                    elif e.status == 403:
                        logging.error(f"Access forbidden for {url}. Status code: {e.status}")
                        return e  # If forbidden, return the exception
                    else:
                        logging.error(f"HTTP error during {method} request: {e.status}")

                except aiohttp.ClientError as e:
                    logging.warning(f"Network or client error during {method} request: {str(e)}")
                    proxy = self.proxy_carousel.get_random_async_proxy()
                    await asyncio.sleep(self.backoff_factor * (2 ** attempt))  # Retry with exponential backoff

                except ValueError as e:
                    logging.error(f"Data validation error: {str(e)}")

                attempt += 1

            # Final fallback after retries are exhausted
            if expect_json:
                return {"status_code": 404, "error": "Request failed after retries"}
            else:
                return "Request failed after retries"

    async def async_post(self, url, expect_json=True, proxy="", **kwargs):
        return await self._async_get_post(url, expect_json=expect_json, method="POST", proxy=proxy, **kwargs)

    async def async_get(self, url, expect_json=True, proxy="", **kwargs):
        return await self._async_get_post(url, expect_json=expect_json, method="GET", proxy=proxy, **kwargs)


def login_required(func):
    def func_wrapper(self, *args, **kwargs):
        if not self.was_login_executed:
            raise LoginRequired('Use login method first')
        else:
            return func(self, *args, **kwargs)

    return func_wrapper


def text_between(text: str, begin: str, end: str) -> str:
    start = text.index(begin) + len(begin)
    end = text.index(end, start)
    return text[start:end]


def texts_between(text: str, begin: str, end: str):
    stop = 0
    while True:
        try:
            start = text.index(begin, stop) + len(begin)
            stop = text.index(end, start)
            yield text[start:stop]
        except ValueError:
            return


def account_id_to_steam_id(account_id: str) -> str:
    first_bytes = int(account_id).to_bytes(4, byteorder = 'big')
    last_bytes = 0x1100001.to_bytes(4, byteorder = 'big')
    return str(struct.unpack('>Q', last_bytes + first_bytes)[0])


def steam_id_to_account_id(steam_id: str) -> str:
    return str(struct.unpack('>L', int(steam_id).to_bytes(8, byteorder = 'big')[4:])[0])


def calculate_gross_price(price_net: Decimal, publisher_fee: Decimal, steam_fee: Decimal = Decimal('0.05')) -> Decimal:
    """Calculate the price including the publisher's fee and the Steam fee.

    Arguments:
        price_net (Decimal): The amount that the seller receives after a market transaction.
        publisher_fee (Decimal): The Publisher Fee is a game specific fee that is determined and collected by the game
            publisher. Most publishers have a `10%` fee - `Decimal('0.10')` with a minimum fee of `$0.01`.
        steam_fee (Decimal): The Steam Transaction Fee is collected by Steam and is used to protect against nominal
            fraud incidents and cover the cost of development of this and future Steam economy features. The fee is
            currently `5%` (with a minimum fee of `$0.01`). This fee may be increased or decreased by Steam in the
            future.
    Returns:
        Decimal: Gross price (including fees) - the amount that the buyer pays during a market transaction
    """
    price_net *= 100
    steam_fee_amount = int(math.floor(max(price_net * steam_fee, 1)))
    publisher_fee_amount = int(math.floor(max(price_net * publisher_fee, 1)))
    price_gross = price_net + steam_fee_amount + publisher_fee_amount
    return Decimal(price_gross) / 100


def calculate_net_price(price_gross: Decimal, publisher_fee: Decimal, steam_fee: Decimal = Decimal('0.05')) -> Decimal:
    """Calculate the price without the publisher's fee and the Steam fee.

    Arguments:
        price_gross (Decimal): The amount that the buyer pays during a market transaction.
        publisher_fee (Decimal): The Publisher Fee is a game specific fee that is determined and collected by the game
            publisher. Most publishers have a `10%` fee - `Decimal('0.10')` with a minimum fee of `$0.01`.
        steam_fee (Decimal): The Steam Transaction Fee is collected by Steam and is used to protect against nominal
            fraud incidents and cover the cost of development of this and future Steam economy features. The fee is
            currently `5%` (with a minimum fee of `$0.01`). This fee may be increased or decreased by Steam in the
            future.
    Returns:
        Decimal: Net price (without fees) - the amount that the seller receives after a market transaction.
    """
    price_gross *= 100
    estimated_net_price = Decimal(int(price_gross / (steam_fee + publisher_fee + 1)))
    estimated_gross_price = calculate_gross_price(estimated_net_price / 100, publisher_fee, steam_fee) * 100

    # Since calculate_gross_price has a math.floor, we could be off a cent or two. Let's check:
    iterations = 0  # Shouldn't be needed, but included to be sure nothing unforeseen causes us to get stuck
    ever_undershot = False
    while estimated_gross_price != price_gross and iterations < 10:
        if estimated_gross_price > price_gross:
            if ever_undershot:
                break
            estimated_net_price -= 1
        else:
            ever_undershot = True
            estimated_net_price += 1

        estimated_gross_price = calculate_gross_price(estimated_net_price / 100, publisher_fee, steam_fee) * 100
        iterations += 1
    return estimated_net_price / 100


def merge_items_with_descriptions_from_inventory(inventory_response: dict, game: GameOptions) -> dict:
    inventory = inventory_response.get('assets', [])
    if not inventory:
        return {}
    descriptions = {get_description_key(description): description for description in inventory_response['descriptions']}
    return merge_items(inventory, descriptions, context_id = game.context_id)


def merge_items_with_descriptions_from_offers(offers_response: dict) -> dict:
    descriptions = {get_description_key(offer): offer for offer in offers_response['response'].get('descriptions', [])}
    received_offers = offers_response['response'].get('trade_offers_received', [])
    sent_offers = offers_response['response'].get('trade_offers_sent', [])
    offers_response['response']['trade_offers_received'] = list(
        map(lambda offer: merge_items_with_descriptions_from_offer(offer, descriptions), received_offers)
    )
    offers_response['response']['trade_offers_sent'] = list(
        map(lambda offer: merge_items_with_descriptions_from_offer(offer, descriptions), sent_offers)
    )
    return offers_response


def merge_items_with_descriptions_from_offer(offer: dict, descriptions: dict) -> dict:
    merged_items_to_give = merge_items(offer.get('items_to_give', []), descriptions)
    merged_items_to_receive = merge_items(offer.get('items_to_receive', []), descriptions)
    offer['items_to_give'] = merged_items_to_give
    offer['items_to_receive'] = merged_items_to_receive
    return offer


def merge_items_with_descriptions_from_listing(listings: dict, ids_to_assets_address: dict, descriptions: dict) -> dict:
    for listing_id, listing in listings.get('sell_listings').items():
        asset_address = ids_to_assets_address[listing_id]
        description = descriptions[asset_address[0]][asset_address[1]][asset_address[2]]
        listing['description'] = description
    return listings


def merge_items(items: List[dict], descriptions: dict, **kwargs) -> dict:
    merged_items = {}

    for item in items:
        description_key = get_description_key(item)
        description = copy.copy(descriptions[description_key])
        item_id = item.get('id') or item['assetid']
        description['contextid'] = item.get('contextid') or kwargs['context_id']
        description['id'] = item_id
        description['amount'] = item['amount']
        merged_items[item_id] = description

    return merged_items


def get_market_listings_from_html(html: str) -> dict:
    document = BeautifulSoup(html, 'html.parser')
    nodes = document.select('div[id=myListings]')[0].findAll('div', {'class': 'market_home_listing_table'})
    sell_listings_dict = {}
    buy_orders_dict = {}

    for node in nodes:
        if 'My sell listings' in node.text:
            sell_listings_dict = get_sell_listings_from_node(node)
        elif 'My listings awaiting confirmation' in node.text:
            sell_listings_awaiting_conf = get_sell_listings_from_node(node)
            for listing in sell_listings_awaiting_conf.values():
                listing['need_confirmation'] = True
            sell_listings_dict.update(sell_listings_awaiting_conf)
        elif 'My buy orders' in node.text:
            buy_orders_dict = get_buy_orders_from_node(node)

    return {'buy_orders': buy_orders_dict, 'sell_listings': sell_listings_dict}


def get_sell_listings_from_node(node: Tag) -> dict:
    sell_listings_raw = node.findAll('div', {'id': re.compile('mylisting_\d+')})
    sell_listings_dict = {}

    for listing_raw in sell_listings_raw:
        spans = listing_raw.select('span[title]')
        listing = {
            'listing_id': listing_raw.attrs['id'].replace('mylisting_', ''),
            'buyer_pay': spans[0].text.strip(),
            'you_receive': spans[1].text.strip()[1:-1],
            'created_on': listing_raw.findAll('div', {'class': 'market_listing_listed_date'})[0].text.strip(),
            'need_confirmation': False,
        }
        sell_listings_dict[listing['listing_id']] = listing

    return sell_listings_dict


def get_market_sell_listings_from_api(html: str) -> dict:
    document = BeautifulSoup(html, 'html.parser')
    sell_listings_dict = get_sell_listings_from_node(document)
    return {'sell_listings': sell_listings_dict}


def get_buy_orders_from_node(node: Tag) -> dict:
    buy_orders_raw = node.findAll('div', {'id': re.compile('mybuyorder_\\d+')})
    buy_orders_dict = {}

    for order in buy_orders_raw:
        qnt_price_raw = order.select('span[class=market_listing_price]')[0].text.split('@')
        order = {
            'order_id': order.attrs['id'].replace('mybuyorder_', ''),
            'quantity': int(qnt_price_raw[0].strip()),
            'price': qnt_price_raw[1].strip(),
            'item_name': order.a.text,
            'game_name': order.select('span[class=market_listing_game_name]')[0].text,
        }
        buy_orders_dict[order['order_id']] = order

    return buy_orders_dict


def get_listing_id_to_assets_address_from_html(html: str) -> dict:
    listing_id_to_assets_address = {}
    regex = "CreateItemHoverFromContainer\( [\w]+, 'mylisting_([\d]+)_[\w]+', ([\d]+), '([\d]+)', '([\d]+)', [\d]+ \);"

    for match in re.findall(regex, html):
        listing_id_to_assets_address[match[0]] = [str(match[1]), match[2], match[3]]

    return listing_id_to_assets_address


def get_description_key(item: dict) -> str:
    return f'{item["classid"]}_{item["instanceid"]}'


def get_key_value_from_url(url: str, key: str, case_sensitive: bool = True) -> str:
    params = urlparse(url).query
    return parse_qs(params)[key][0] if case_sensitive else CaseInsensitiveDict(parse_qs(params))[key][0]


def load_credentials():
    dirname = os.path.dirname(os.path.abspath(__file__))
    with open(f'{dirname}/../secrets/credentials.pwd', 'r') as f:
        return [Credentials(line.split()[0], line.split()[1], line.split()[2]) for line in f]


class Credentials:
    def __init__(self, login: str, password: str, api_key: str):
        self.login = login
        self.password = password
        self.api_key = api_key


def ping_proxy(proxies: dict):
    try:
        requests.get('https://steamcommunity.com/', proxies=proxies)
        return True
    except Exception:
        raise ProxyConnectionError('Proxy not working for steamcommunity.com')


def create_cookie(name: str, cookie: str, domain: str) -> dict:
    return {'name': name, 'value': cookie, 'domain': domain}
