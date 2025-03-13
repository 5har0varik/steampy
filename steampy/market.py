import json
import ast
import time
import asyncio
import aiohttp

import bs4

import urllib.parse
from decimal import Decimal
from http import HTTPStatus

from steampy.confirmation import ConfirmationExecutor
from steampy.exceptions import ApiException, TooManyRequests
from steampy.models import Currency, SteamUrl, GameOptions
from steampy.utils import (
    text_between,
    get_listing_id_to_assets_address_from_html,
    get_market_listings_from_html,
    merge_items_with_descriptions_from_listing,
    get_market_sell_listings_from_api,
    login_required,
    SafeSession,
    AsyncSession
)


class SteamMarket:
    def __init__(self, session: SafeSession, asyncSession: AsyncSession) -> None:
        self._session = session
        self._async_session = asyncSession
        self._steam_guard = None
        self._session_id = None
        self.was_login_executed = False

    def _set_login_executed(self, steamguard: dict, session_id: str) -> None:
        self._steam_guard = steamguard
        self._session_id = session_id
        self.was_login_executed = True

    def fetch_price(self, item_hash_name: str, game: GameOptions, currency: str = Currency.USD) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/priceoverview/'
        params = {'country': 'PL',
                  'currency': currency.value,
                  'appid': game.app_id,
                  'market_hash_name': item_hash_name}
        response = self._session.safe_get(url, expect_json=True, params=params)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    def fetch_price_offer(self, item_hash_name: str, game: GameOptions, currency: str = Currency.USD) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_hash_name + \
              "/render/?query=&start=0&count=20&country=UA&language=english&currency=18"
        item_market_url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_hash_name
        headers = {'Referer': item_market_url}
        response = self._session.safe_get(url, expect_json=True, headers=headers)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    @login_required
    def fetch_price_history(self, item_market_url: str, game: GameOptions, get_id=False) -> tuple:
        url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_market_url
        response = self._session.safe_get(url, expect_json=False)
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

    """
    async def fetch_price_history_async(self, item_market_url_list: list, game: GameOptions, get_id=False):
        tasks = []
        for item_market_url in item_market_url_list:
            url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_market_url
            tasks.append(self._async_session.async_get(url, expect_json=False))

        return await asyncio.gather(*tasks, return_exceptions=True)

    def fetch_price_history_async_run(self, item_market_url_list: list, game: GameOptions, get_id=False) -> list:
        results_data = []
        results = asyncio.run(self.fetch_price_history_async(item_market_url_list, game, get_id=False))
        for response in results:
            if isinstance(response, str):
                data_string = ""
                if 'var line1=' in response:
                    data_string = text_between(response, 'var line1=', 'g_timePriceHistoryEarliest = new Date();')
                else:
                    if get_id:
                        if 'Market_LoadOrderSpread' in response:
                            id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                            results_data.append(([], False, int(id_string)))
                        else:
                            results_data.append(([], False, 0))
                    else:
                        results_data.append(([], False))
                    continue
                data_string = data_string[:data_string.find(';')]
                data_string = ast.literal_eval(data_string)
                if get_id:
                    if 'Market_LoadOrderSpread' in response:
                        id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, int(id_string)))
                    else:
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, 0))
                else:
                    results_data.append(( data_string, "( Not Usable in Crafting )" in response))
            else:
                print(response)
                results_data.append([])
        return results_data
    """

    async def fetch_price_history_async(self, item_market_url_list: list, game: GameOptions, get_id=False):
        tasks = []
        for item_market_url in item_market_url_list:
            url = SteamUrl.COMMUNITY_URL + '/market/listings/' + game.app_id + '/' + item_market_url
            tasks.append(self._async_session.async_get(url, expect_json=False))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        results_data = []
        for response in results:
            if isinstance(response, str):
                data_string = ""
                if 'var line1=' in response:
                    data_string = text_between(response, 'var line1=', 'g_timePriceHistoryEarliest = new Date();')
                else:
                    if get_id:
                        if 'Market_LoadOrderSpread' in response:
                            id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                            results_data.append(([], False, int(id_string)))
                        else:
                            results_data.append(([], False, 0))
                    else:
                        results_data.append(([], False))
                    continue
                data_string = data_string[:data_string.find(';')]
                data_string = ast.literal_eval(data_string)
                if get_id:
                    if 'Market_LoadOrderSpread' in response:
                        id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, int(id_string)))
                    else:
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, 0))
                else:
                    results_data.append(( data_string, "( Not Usable in Crafting )" in response))
            else:
                print(response)
                results_data.append([])
        return results_data

    def fetch_price_history_async_run(self, item_market_url_list: list, game: GameOptions, get_id=False) -> list:
        results_data = []
        results = asyncio.run(self.fetch_price_history_async(item_market_url_list, game, get_id=False))
        for response in results:
            if isinstance(response, str):
                data_string = ""
                if 'var line1=' in response:
                    data_string = text_between(response, 'var line1=', 'g_timePriceHistoryEarliest = new Date();')
                else:
                    if get_id:
                        if 'Market_LoadOrderSpread' in response:
                            id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                            results_data.append(([], False, int(id_string)))
                        else:
                            results_data.append(([], False, 0))
                    else:
                        results_data.append(([], False))
                    continue
                data_string = data_string[:data_string.find(';')]
                data_string = ast.literal_eval(data_string)
                if get_id:
                    if 'Market_LoadOrderSpread' in response:
                        id_string = text_between(response, 'Market_LoadOrderSpread( ', ' );')
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, int(id_string)))
                    else:
                        results_data.append(( data_string, "( Not Usable in Crafting )" in response, 0))
                else:
                    results_data.append(( data_string, "( Not Usable in Crafting )" in response))
            else:
                print(response)
                results_data.append([])
        return results_data

    @login_required
    def fetch_item_orders_histogram(self, item_nameid: str, item_market_url: str, currency: str = Currency.USD) -> dict:
        url = SteamUrl.COMMUNITY_URL + '/market/itemordershistogram'
        params = {'country': 'UA',
                  'language': 'english',
                  'currency': currency.value,
                  'item_nameid': item_nameid,
                  'two_factor': '0'}
        #self._session.headers.update({'Referer': item_market_url})
        headers = {'Referer': item_market_url}
        response = self._session.safe_get(url, expect_json=True, params=params, headers=headers)
        if response.status_code == 429:
            raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
        return response.json()

    """
    async def fetch_item_orders_histogram_async(self, item_nameid_list: list, item_market_url_list: list, currency: str = Currency.USD):
        tasks = []
        for item_nameid, item_market_url in zip(item_nameid_list, item_market_url_list):
            url = SteamUrl.COMMUNITY_URL + '/market/itemordershistogram'
            params = {'country': 'UA',
                  'language': 'english',
                  'currency': currency.value,
                  'item_nameid': item_nameid,
                  'two_factor': '0'}
            headers = {'Referer': item_market_url}
            tasks.append(self._async_session.async_get(url, params=params, headers=headers))
        return await asyncio.gather(*tasks, return_exceptions=True)


    def fetch_item_orders_histogram_async_run(self,  item_nameid_list: list, item_market_url_list: list, currency: str = Currency.USD):
        results = asyncio.run(self.fetch_item_orders_histogram_async(item_nameid_list, item_market_url_list))

        results_data = []
        for response in results:
            #if response.status_code == 429:
            #    raise TooManyRequests("You can fetch maximum 20 prices in 60s period")
            # results_data.append(response.json())
            results_data.append(response)
        return  results_data
    """

    async def fetch_item_orders_histogram_async(self, item_nameid_list: list, item_market_url_list: list,
                                                currency: str = Currency.USD):
        """
        Asynchronous method to fetch item order histograms.
        """
        tasks = []
        for item_nameid, item_market_url in zip(item_nameid_list, item_market_url_list):
            url = SteamUrl.COMMUNITY_URL + '/market/itemordershistogram'
            params = {
                'country': 'UA',
                'language': 'english',
                'currency': currency.value,
                'item_nameid': item_nameid,
                'two_factor': '0'
            }
            headers = {'Referer': item_market_url}
            tasks.append(self._async_session.async_get(url, params=params, headers=headers))

        return await asyncio.gather(*tasks, return_exceptions=True)

    def fetch_item_orders_histogram_async_run(self, item_nameid_list: list, item_market_url_list: list,
                                              currency: str = Currency.USD):
        """
        This method should now rely on the external class to manage the event loop.
        """
        return self.fetch_item_orders_histogram_async(item_nameid_list, item_market_url_list, currency)

    @login_required
    def get_my_market_listings(self) -> dict:
        response = self._session.safe_get("%s/market" % SteamUrl.COMMUNITY_URL, expect_json=False)
        if response.status_code != 200:
            raise ApiException("There was a problem getting the listings. http code: %s" % response.status_code)
        assets_descriptions = json.loads(text_between(response.text, 'var g_rgAssets = ', ';\n'))
        listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(response.text)
        listings = get_market_listings_from_html(response.text)
        listings = merge_items_with_descriptions_from_listing(
            listings, listing_id_to_assets_address, assets_descriptions
        )

        if '<span id="tabContentsMyActiveMarketListings_end">' in response.text:
            n_showing = int(text_between(response.text, '<span id="tabContentsMyActiveMarketListings_end">', '</span>'))
            n_total = int(
                text_between(response.text, '<span id="tabContentsMyActiveMarketListings_total">', '</span>').replace(
                    ',', ''
                )
            )

            if n_showing < n_total < 1000:
                url = f'{SteamUrl.COMMUNITY_URL}/market/mylistings/render/?query=&start={n_showing}&count={-1}'
                response = self._session.safe_get(url, expect_json=True)
                if response.status_code != HTTPStatus.OK:
                    raise ApiException(f'There was a problem getting the listings. HTTP code: {response.status_code}')
                jresp = response.json()
                listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(jresp.get('hovers'))
                listings_2 = get_market_sell_listings_from_api(jresp.get('results_html'))
                listings_2 = merge_items_with_descriptions_from_listing(
                    listings_2, listing_id_to_assets_address, jresp.get('assets')
                )
                listings['sell_listings'] = {**listings['sell_listings'], **listings_2['sell_listings']}
            else:
                for i in range(0, n_total, 100):
                    url = f'{SteamUrl.COMMUNITY_URL}/market/mylistings/?query=&start={n_showing + i}&count={100}'
                    response = self._session.safe_get(url, expect_json=True)
                    if response.status_code != HTTPStatus.OK:
                        raise ApiException(
                            f'There was a problem getting the listings. HTTP code: {response.status_code}'
                        )
                    jresp = response.json()
                    listing_id_to_assets_address = get_listing_id_to_assets_address_from_html(jresp.get('hovers'))
                    listings_2 = get_market_sell_listings_from_api(jresp.get('results_html'))
                    listings_2 = merge_items_with_descriptions_from_listing(
                        listings_2, listing_id_to_assets_address, jresp.get('assets')
                    )
                    listings['sell_listings'] = {**listings['sell_listings'], **listings_2['sell_listings']}

        return listings

    @login_required
    def create_sell_order(self, assetid: str, game: GameOptions, money_to_receive: str) -> dict:
        data = {
            'assetid': assetid,
            'sessionid': self._session_id,
            'contextid': game.context_id,
            'appid': game.app_id,
            'amount': 1,
            'price': money_to_receive,
        }
        headers = {'Referer': f'{SteamUrl.COMMUNITY_URL}/profiles/{self._steam_guard["steamid"]}/inventory'}
        response = self._session.safe_post(f'{SteamUrl.COMMUNITY_URL}/market/sellitem/', expect_json=True, data=data,
                                           headers=headers).json()
        if response.get("needs_mobile_confirmation"):
            r = self._confirm_sell_listing(assetid)
            while 'success' not in r or not r['success']:
                print(r)
                time.sleep(5)
                r = self._confirm_sell_listing(assetid)
            return r
        return response

    @login_required
    def create_buy_order(
        self,
        market_name: str,
        price_single_item: str,
        quantity: int,
        game: GameOptions,
        currency: Currency = Currency.USD,
    ) -> dict:
        data = {
            'sessionid': self._session_id,
            'currency': currency.value,
            'appid': game.app_id,
            'market_hash_name': market_name,
            'price_total': str(Decimal(price_single_item) * Decimal(quantity)),
            'quantity': quantity,
        }
        headers = {
            'Referer': f'{SteamUrl.COMMUNITY_URL}/market/listings/{game.app_id}/{urllib.parse.quote(market_name)}'
        }
        response = None
        attempts = 5
        while attempts > 0:
            response = self._session.safe_post(f'{SteamUrl.COMMUNITY_URL}/market/createbuyorder/', expect_json=True,
                                               data=data, headers=headers).json()
            if response.get("success") == 1:
                return response
            elif response.get("success") == 29:
                print(response)
                return response
            elif response.get("success") == 16:
                print(response)
                attempts -= 1
                time.sleep(5)
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

        if response is None:
            raise ApiException("There was a problem creating the order. Are you using the right currency? success: %s"
                               % response)
        return response

    @login_required
    def buy_item(
        self,
        market_name: str,
        market_id: str,
        price: int,
        fee: int,
        game: GameOptions,
        currency: Currency = Currency.USD,
    ) -> dict:
        data = {
            'sessionid': self._session_id,
            'currency': currency.value,
            'subtotal': price - fee,
            'fee': fee,
            'total': price,
            'quantity': '1',
        }
        headers = {
            'Referer': f'{SteamUrl.COMMUNITY_URL}/market/listings/{game.app_id}/{urllib.parse.quote(market_name)}'
        }
        response = self._session.safe_post(f'{SteamUrl.COMMUNITY_URL}/market/buylisting/{market_id}', expect_json=True,
                                           data=data, headers=headers).json()
        try:
            if (success := response['wallet_info']['success']) != 1:
                raise ApiException(
                    f'There was a problem buying this item. Are you using the right currency? success: {success}'
                )
        except Exception:
            raise ApiException(f'There was a problem buying this item. Message: {response.get("message")}')

        return response

    @login_required
    def cancel_sell_order(self, sell_listing_id: str) -> None:
        data = {'sessionid': self._session_id}
        headers = {'Referer': f'{SteamUrl.COMMUNITY_URL}/market/'}
        url = f'{SteamUrl.COMMUNITY_URL}/market/removelisting/{sell_listing_id}'
        response = self._session.safe_post(url, expect_json=True, data=data, headers=headers)
        if response.status_code != 200:
            raise ApiException("There was a problem removing the listing. http code: %s" % response.status_code)

    @login_required
    def get_sell_order(self, sell_listing_id: str) -> dict:
        data = {'sessionid': self._session_id}
        headers = {'Referer': f'{SteamUrl.COMMUNITY_URL}/market'}
        url = "%s/market/getbuyorderstatus/?sessionid=%s&buy_orderid=%s" % (SteamUrl.COMMUNITY_URL, self._session_id,
                                                                            sell_listing_id)
        response = self._session.safe_post(url, expect_json=True, data=data, headers=headers)
        if response.status_code != 200:
            raise ApiException("There was a problem removing the listing. http code: %s" % response.status_code)
        return response.json()

    @login_required
    def cancel_buy_order(self, buy_order_id) -> dict:
        data = {'sessionid': self._session_id, 'buy_orderid': buy_order_id}
        headers = {'Referer': f'{SteamUrl.COMMUNITY_URL}/market'}
        response = None
        attempts = 5
        while attempts > 0:
            response = self._session.safe_post(f'{SteamUrl.COMMUNITY_URL}/market/cancelbuyorder/', expect_json=True,
                                               data=data, headers=headers).json()
            if response.get("success") != 1:
                print("There was a problem canceling the order. success: %s" % response.get("success"))
                attempts -= 1
                time.sleep(5*(5 - attempts))
            else:
                break

        # if response.get("success") != 1:
        #     raise ApiException("There was a problem canceling the order. success: %s" % response.get("success"))
        return response

    @login_required
    def get_latest_trade_hist(self, request_size=10, request_start=0):
        headers = {"Referer": SteamUrl.COMMUNITY_URL + "/market"}
        response = None
        url = "%s/market/myhistory/render/?query=&start=%s&count=%s" % \
              (SteamUrl.COMMUNITY_URL, str(request_start), str(request_size))
        response = None
        attempts = 10
        while attempts > 0:
            response = self._session.safe_get(url, expect_json=True, headers=headers)
            if response.json()["total_count"] == 0:
                time.sleep(5)
                attempts -= 1
            else:
                break
        if response.json()["total_count"] == 0:
            raise ApiException("Problem while obtaining latest trade hist: zero size ", response.text)

        prices = []
        soup = bs4.BeautifulSoup(response.json()["results_html"], "html.parser")
        all_rows = soup.find_all("div", class_="market_listing_row market_recent_listing_row")
        for item in all_rows:
            if item.find("div", class_="market_listing_whoactedwith_name_block") is not None:
                purchase_sum = float(item.find("span", class_="market_listing_price").getText().replace("\t", "").
                                     replace("\n", "").replace("\r", "").replace(",", ".").replace(" ", "")[:-1])
                purchase_string_raw = item.find("div", class_="market_listing_listed_date_combined").getText(). \
                    replace("\t", "").replace("\n", "").replace("\r", "")
                purchase_string = purchase_string_raw[purchase_string_raw.find(":") + 2:]
                if "-" in item.find("div", class_="market_listing_left_cell market_listing_gainorloss").getText():
                    prices.append({"action": "sell", "price": purchase_sum, "date_string": purchase_string})
                elif "+" in item.find("div", class_="market_listing_left_cell market_listing_gainorloss").getText():
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

    @login_required
    def search(self, request_start, game: GameOptions, request_size=100):
        """Parse search"""
        headers = {"Referer": SteamUrl.COMMUNITY_URL + "/market/search?appid=" + str(game.app_id)}
        response = None
        url = "%s/market/search/render/?query=&start=%s&count=%s&search_descriptions=0&sort_column=name&sort_dir=asc&appid=%s&norender=1" % \
              (SteamUrl.COMMUNITY_URL, str(request_start), str(request_size), str(game.app_id))
        print(url)
        attempts = 5
        while attempts > 0:
            response = self._session.safe_get(url, expect_json=True, headers=headers)
            if not response.json()["success"]:
                time.sleep(5)
                attempts -= 1
            else:
                break
        if not response.json()["success"]:
            raise ApiException("Problem while obtaining latest trade hist: zero size ", response.text)

        return response.json()["results"]

    def _confirm_sell_listing(self, asset_id: str) -> dict:
        con_executor = ConfirmationExecutor(
            self._steam_guard['identity_secret'], self._steam_guard['steamid'], self._session
        )
        return con_executor.confirm_sell_listing(asset_id)
