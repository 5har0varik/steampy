import urllib.parse
import json
import ast
import time
import requests

from decimal import Decimal

import bs4
from requests import Session, exceptions
from steampy.confirmation import ConfirmationExecutor
from steampy.exceptions import ApiException, TooManyRequests, LoginRequired
from steampy.models import Currency, SteamUrl, GameOptions
from steampy.utils import text_between, get_listing_id_to_assets_address_from_html, get_market_listings_from_html, \
    merge_items_with_descriptions_from_listing, get_market_sell_listings_from_api


def login_required(func):
    def func_wrapper(self, *args, **kwargs):
        if not self.was_login_executed:
            raise LoginRequired('Use login method first on SteamClient')
        else:
            return func(self, *args, **kwargs)

    return func_wrapper


class SteamMarket:
    def __init__(self, session: Session):
        self._session = session
        self._steam_guard = None
        self._session_id = None
        self.was_login_executed = False

    def _set_login_executed(self, steamguard: dict, session_id: str):
        self._steam_guard = steamguard
        self._session_id = session_id
        self.was_login_executed = True
        
    def _safe_get(self, url, params=None, headers=None):
        if not params:
            params = {}
        if not headers:
            headers = {}
        response = type('obj', (object,), {'status_code': None, 'text': None})
        pause_time = 0
        for i in range(100):
            try:
                response = self._session.get(url, params=params, headers=headers)
                pause_time += 1
                response.raise_for_status()
            except exceptions.HTTPError as errh:
                print("Steampy Http Error:", errh)
                time.sleep(pause_time)
                continue
            except exceptions.ConnectionError as errc:
                print("Steampy Error Connecting:", errc)
                time.sleep(pause_time)
                continue
            except exceptions.Timeout as errt:
                print("Steampy Timeout Error:", errt)
                time.sleep(pause_time)
                continue
            except exceptions.SSLError as errs:
                print("Steampy SSL Error:", errs)
                time.sleep(pause_time)
                continue
            break
        try:
            data = response.json()
        except exceptions.JSONDecodeError as errj:
            print("Steampy JSON Error:", errj)
            time.sleep(1)
            response = type('obj', (object,), {'status_code': None, 'text': None})
        return response

    def _safe_post(self, url, params=None, headers=None, data=None, use_proxy=False, is_json=True):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

        repeats = 10
        if headers is None:
            headers = {}
        if params is None:
            params = {}
        if data is None:
            data = {}
        response = type('obj', (object,), {'status_code': None, 'text': None})
        pause_time = 0
        for i in range(repeats):
            if use_proxy:
                proxy = self.proxy.get_proxy()
            else:
                proxy = {}
            try:
                response = self._session.post(url, data=data, params=params, proxies=proxy, headers=headers)
                pause_time += 1
                response.raise_for_status()
            except exceptions.HTTPError as errh:
                print("Steampy Http Error:", errh)
                if errh.response.status_code == requests.codes.TOO_MANY_REQUESTS:
                    response = MockResponse({"status_code": errh.response.status_code}, errh.response.status_code)
                    return response
                time.sleep(pause_time)
                continue
            except exceptions.ConnectionError as errc:
                print("Steampy Error Connecting:", errc)
                time.sleep(pause_time)
                continue
            except exceptions.Timeout as errt:
                print("Steampy Timeout Error:", errt)
                time.sleep(pause_time)
                continue
            except exceptions.SSLError as errs:
                print("Steampy SSL Error:", errs)
                time.sleep(pause_time)
                continue
            if is_json:
                try:
                    data = response.json()
                except exceptions.JSONDecodeError as errj:
                    print("Steampy JSON Error:", errj)
                    time.sleep(pause_time)
                    continue
            return response
        response = MockResponse({"status_code": "Tried for " + str(repeats) + " times"}, 404)
        return response

    def fetch_price(self, item_hash_name: str, game: GameOptions, currency: str = Currency.USD) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/priceoverview/'
        params = {'country': 'PL',
                  'currency': currency.value,
                  'appid': game.app_id,
                  'market_hash_name': item_hash_name}
        response = self._session.get(url, params=params)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    @login_required
    def fetch_price_history_old(self, item_hash_name: str, game: GameOptions) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/pricehistory/'
        params = {'country': 'PL',
                  'appid': game.app_id,
                  'market_hash_name': item_hash_name}
        response = self._session.get(url, params=params)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    @login_required
    def fetch_price_history(self, item_market_url: str, game: GameOptions, get_id=False) -> tuple:
        url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_market_url
        response = self._session.get(url)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        data_string = ""
        if 'var line1=' in response.text:
            data_string = text_between(response.text, 'var line1=', 'g_timePriceHistoryEarliest = new Date();')
        else:
            if get_id:
                if 'Market_LoadOrderSpread' in response.text:
                    id_string = text_between(response.text, 'Market_LoadOrderSpread( ', ' );')
                    return [], False, int(id_string)
                else:
                    return [], False, 0
            else:
                return [], False
        data_string = data_string[:data_string.find(';')]
        data_string = ast.literal_eval(data_string)
        if get_id:
            if 'Market_LoadOrderSpread' in response.text:
                id_string = text_between(response.text, 'Market_LoadOrderSpread( ', ' );')
                return data_string, "( Not Usable in Crafting )" in response.text, int(id_string)
            else:
                return data_string, "( Not Usable in Crafting )" in response.text, 0
        else:
            return data_string, "( Not Usable in Crafting )" in response.text
    
    @login_required
    def fetch_item_orders_histogram(self, item_nameid: str, item_market_url: str, currency: str = Currency.USD) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/itemordershistogram'
        params = {'country': 'UA',
                  'language': 'english',
                  'currency': currency.value,
                  'item_nameid': item_nameid,
                  'two_factor': '0'}
        self._session.headers.update({'Referer': item_market_url})
        response = self._safe_get(url, params=params)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    @login_required
    def get_my_market_listings(self) -> dict:
        response = self._session.get("%s/market" % SteamUrl.COMMUNITY_URL)
        if response.status_code != 200:
            raise ApiException("There was a problem getting the listings. http code: %s" % response.status_code)
        assets_descriptions = json.loads(text_between(response.text, "var g_rgAssets = ", ";\r\n"))
        listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(response.text)
        listings = get_market_listings_from_html(response.text)
        listings = merge_items_with_descriptions_from_listing(listings, listing_id_to_assets_address,
                                                              assets_descriptions)
        if '<span id="tabContentsMyActiveMarketListings_end">' in response.text:
            n_showing = int(text_between(response.text, '<span id="tabContentsMyActiveMarketListings_end">', '</span>'))
            n_total = int(text_between(response.text, '<span id="tabContentsMyActiveMarketListings_total">', '</span>').replace(',',''))
            if n_showing < n_total < 1000:
                url = "%s/market/mylistings/render/?query=&start=%s&count=%s" % (SteamUrl.COMMUNITY_URL, n_showing, -1)
                response = self._session.get(url)
                if response.status_code != 200:
                    raise ApiException("There was a problem getting the listings. http code: %s" % response.status_code)
                jresp = response.json()
                listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(jresp.get("hovers"))
                listings_2 = get_market_sell_listings_from_api(jresp.get("results_html"))
                listings_2 = merge_items_with_descriptions_from_listing(listings_2, listing_id_to_assets_address,
                                                                        jresp.get("assets"))
                listings["sell_listings"] = {**listings["sell_listings"], **listings_2["sell_listings"]}
            else:
                for i in range(0, n_total, 100):
                    url = "%s/market/mylistings/?query=&start=%s&count=%s" % (SteamUrl.COMMUNITY_URL, n_showing + i, 100)
                    response = self._session.get(url)
                    if response.status_code != 200:
                        raise ApiException("There was a problem getting the listings. http code: %s" % response.status_code)
                    jresp = response.json()
                    listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(jresp.get("hovers"))
                    listings_2 = get_market_sell_listings_from_api(jresp.get("results_html"))
                    listings_2 = merge_items_with_descriptions_from_listing(listings_2, listing_id_to_assets_address,
                                                                            jresp.get("assets"))
                    listings["sell_listings"] = {**listings["sell_listings"], **listings_2["sell_listings"]}
        return listings

    @login_required
    def create_sell_order(self, assetid: str, game: GameOptions, money_to_receive: str) -> dict:
        data = {
            "assetid": assetid,
            "sessionid": self._session_id,
            "contextid": game.context_id,
            "appid": game.app_id,
            "amount": 1,
            "price": money_to_receive
        }
        headers = {'Referer': "%s/profiles/%s/inventory" % (SteamUrl.COMMUNITY_URL, self._steam_guard['steamid'])}
        response = self._safe_post(SteamUrl.COMMUNITY_URL + "/market/sellitem/", data=data, headers=headers).json()
        if response.get("needs_mobile_confirmation"):
            r = self._confirm_sell_listing(assetid)
            while 'success' not in r or not r['success']:
                print(r)
                time.sleep(5)
                r = self._confirm_sell_listing(assetid)
            return r
        return response

    @login_required
    def create_buy_order(self, market_name: str, price_single_item: str, quantity: int, game: GameOptions,
                         currency: Currency = Currency.USD) -> dict:
        data = {
            "sessionid": self._session_id,
            "currency": currency.value,
            "appid": game.app_id,
            "market_hash_name": market_name,
            "price_total": str(Decimal(price_single_item) * Decimal(quantity)),
            "quantity": quantity
        }
        headers = {'Referer': "%s/market/listings/%s/%s" % (SteamUrl.COMMUNITY_URL, game.app_id, 
                                                            urllib.parse.quote(market_name))}
        response = None
        attempts = 5
        while attempts > 0:
            try:
                response = self._safe_post(SteamUrl.COMMUNITY_URL + "/market/createbuyorder/", data=data,
                                              headers=headers).json()
            except exceptions.JSONDecodeError as errj:
                print("Steampy JSON Error:", errj)
                time.sleep(5)
                attempts -= 1
                continue
            if response.get("success") == 1:
                return response
            elif response.get("success") == 29:
                print(response)
                return response
            elif response.get("success") == 40:
                print(response)
                attempts -= 1
                time.sleep(5)
            if response.get("success") == 107:
                print(response)
                attempts -= 1
                time.sleep(5)
            else:
                break

        if response is None or response.get("success") != 1:
            raise ApiException("There was a problem creating the order. Are you using the right currency? success: %s"
                               % response)
        return response

    @login_required
    def buy_item(self, market_name: str, market_id: str, price: int, fee: int, game: GameOptions,
                 currency: Currency = Currency.USD) -> dict:
        data = {
            "sessionid": self._session_id,
            "currency": currency.value,
            "subtotal" : price - fee,
            "fee" : fee,
            "total" : price,
            "quantity": '1'
        }
        headers = {'Referer': "%s/market/listings/%s/%s" % (SteamUrl.COMMUNITY_URL, game.app_id,
                                                            urllib.parse.quote(market_name))}
        response = self._session.post(SteamUrl.COMMUNITY_URL + "/market/buylisting/" + market_id, data=data,
                                      headers=headers).json()
        try:
            if response["wallet_info"]["success"] != 1:
                raise ApiException("There was a problem buying this item. Are you using the right currency? success: %s"
                                   % response['wallet_info']['success'])
        except:
            raise ApiException("There was a problem buying this item. Message: %s"
                               % response.get("message"))
        return response

    @login_required
    def cancel_sell_order(self, sell_listing_id: str) -> None:
        data = {"sessionid": self._session_id}
        headers = {'Referer': SteamUrl.COMMUNITY_URL + "/market/"}
        url = "%s/market/removelisting/%s" % (SteamUrl.COMMUNITY_URL, sell_listing_id)
        response = self._safe_post(url, data=data, headers=headers)
        if response.status_code != 200:
            raise ApiException("There was a problem removing the listing. http code: %s" % response.status_code)

    @login_required
    def get_sell_order(self, sell_listing_id: str) -> dict:
        data = {"sessionid": self._session_id}
        headers = {'Referer': SteamUrl.COMMUNITY_URL + "/market/"}
        url = "%s/market/getbuyorderstatus/?sessionid=%s&buy_orderid=%s" % (SteamUrl.COMMUNITY_URL, self._session_id, sell_listing_id)
        response = self._session.post(url, data=data, headers=headers)
        if response.status_code != 200:
            raise ApiException("There was a problem removing the listing. http code: %s" % response.status_code)
        return response.json()

    @login_required
    def cancel_buy_order(self, buy_order_id) -> dict:
        data = {
            "sessionid": self._session_id,
            "buy_orderid": buy_order_id
        }
        headers = {"Referer": SteamUrl.COMMUNITY_URL + "/market"}
        response = None
        attempts = 5
        while attempts > 0:
            try:
                response = self._safe_post(SteamUrl.COMMUNITY_URL + "/market/cancelbuyorder/", data=data, headers=headers).json()
            except exceptions.JSONDecodeError as errj:
                print("Steampy JSON Error:", errj)
                attempts -= 1
                time.sleep(5*(5 - attempts))
                continue
            if response.get("success") != 1:
                print("There was a problem canceling the order. success: %s" % response.get("success"))
                attempts -= 1
                time.sleep(5*(5 - attempts))
            else:
                break

        if response.get("success") != 1:
            raise ApiException("There was a problem canceling the order. success: %s" % response.get("success"))
        return response

    @login_required
    def get_latest_trade_hist(self, request_size=10, request_start=0):
        headers = {"Referer": SteamUrl.COMMUNITY_URL + "/market"}
        response = None
        url = "%s/market/myhistory/?query=&start=%s&count=%s" % \
              (SteamUrl.COMMUNITY_URL, str(request_start), str(request_size))
        response = self._safe_get(url, headers=headers)

        prices = []
        soup = bs4.BeautifulSoup(response.json()["results_html"], "html.parser")
        all_rows = soup.find_all("div", class_="market_listing_row market_recent_listing_row")
        for item in all_rows:
            if item.find("div", class_="market_listing_whoactedwith_name_block") is not None:
                purchase_sum = float(item.find("span", class_="market_listing_price").getText().replace("\t", ""). \
                                     replace("\n", "").replace("\r", "").replace(",", ".").replace(" ", "")[:-1])
                purchase_string_raw = item.find("div", class_="market_listing_listed_date_combined").getText(). \
                    replace("\t", "").replace("\n", "").replace("\r", "")
                purchase_string = purchase_string_raw[purchase_string_raw.find(":") + 2:]
                if "Buyer" in item.find("div", class_="market_listing_whoactedwith_name_block").getText():
                    prices.append({"action": "sell", "price": purchase_sum, "date_string": purchase_string})
                elif "Seller" in item.find("div", class_="market_listing_whoactedwith_name_block").getText():
                    prices.append({"action": "buy", "price": purchase_sum, "date_string": purchase_string})

        json_data = response.json()["assets"]
        items = {}
        for appid, itemslist in json_data.items():
            for contextid, item in itemslist.items():
                index = 0
                for k, v in item.items():
                    if (v["status"] != 2) & (v["status"] != 8):
                        items[k] = v
                        items[k]["action"] = prices[index]["action"]
                        items[k]["price"] = prices[index]["price"]
                        items[k]["date_string"] = prices[index]["date_string"]
                        index += 1

        return items

    def _confirm_sell_listing(self, asset_id: str) -> dict:
        con_executor = ConfirmationExecutor(self._steam_guard['identity_secret'], self._steam_guard['steamid'],
                                            self._session)
        return con_executor.confirm_sell_listing(asset_id)
