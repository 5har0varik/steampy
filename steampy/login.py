import base64
import time
import requests
from steampy import guard
import rsa
from steampy.models import SteamUrl
from steampy.exceptions import InvalidCredentials, CaptchaRequired


class LoginExecutor:

    def __init__(self, username: str, password: str, shared_secret: str, session: requests.Session) -> None:
        self.username = username
        self.password = password
        self.one_time_code = ''
        self.shared_secret = shared_secret
        self.session = session
        self.refresh_token = ''

    def login(self) -> requests.Session:
        login_response = self._send_login_request()
        if len(login_response.json()['response']) == 0:
            raise ApiException('No response received from Steam API. Please try again later.')
        self._check_for_captcha(login_response)
        self._update_steam_guard(login_response)
        finallized_response = self._finallize_login()
        self._perform_redirects(finallized_response.json())
        self.set_sessionid_cookies()
        return self.session

    def _send_login_request(self) -> requests.Response:
        rsa_params = self._fetch_rsa_params()
        encrypted_password = self._encrypt_password(rsa_params)
        rsa_timestamp = rsa_params['rsa_timestamp']
        request_data = self._prepare_login_request_data(encrypted_password, rsa_timestamp)
        return self._api_call('POST', 'IAuthenticationService', 'BeginAuthSessionViaCredentials', params=request_data)

    def set_sessionid_cookies(self):
        sessionid = self.session.cookies.get_dict()['sessionid']
        community_domain = SteamUrl.COMMUNITY_URL[8:]
        store_domain = SteamUrl.STORE_URL[8:]
        community_cookie = self._create_session_id_cookie(sessionid, community_domain)
        store_cookie = self._create_session_id_cookie(sessionid, store_domain)
        self.session.cookies.set(**community_cookie)
        self.session.cookies.set(**store_cookie)

    @staticmethod
    def _create_session_id_cookie(sessionid: str, domain: str) -> dict:
        return {"name": "sessionid",
                "value": sessionid,
                "domain": domain}

    def _fetch_rsa_params(self, current_number_of_repetitions: int = 0) -> dict:

        self.session.post(SteamUrl.COMMUNITY_URL)
        request_data = {'account_name': self.username}
        response = self._api_call('GET', 'IAuthenticationService', 'GetPasswordRSAPublicKey', params=request_data)

        if response.status_code == HTTPStatus.OK and 'response' in response.json():
            key_data = response.json()['response']
            # Steam may return an empty "response" value even if the status is 200
            if 'publickey_mod' in key_data and 'publickey_exp' in key_data and 'timestamp' in key_data:
                rsa_mod = int(key_data['publickey_mod'], 16)
                rsa_exp = int(key_data['publickey_exp'], 16)
                return {'rsa_key': PublicKey(rsa_mod, rsa_exp), 'rsa_timestamp': key_data['timestamp']}


        maximal_number_of_repetitions = 5
        key_response = self.session.post(SteamUrl.COMMUNITY_URL + '/login/getrsakey/',
                                         data={'username': self.username}).json()
        try:
            rsa_mod = int(key_response['publickey_mod'], 16)
            rsa_exp = int(key_response['publickey_exp'], 16)
            rsa_timestamp = key_response['timestamp']
            return {'rsa_key': rsa.PublicKey(rsa_mod, rsa_exp),
                    'rsa_timestamp': rsa_timestamp}
        except KeyError:
            if current_number_of_repetitions < maximal_number_of_repetitions:
                return self._fetch_rsa_params(current_number_of_repetitions + 1)
            else:
                raise ValueError('Could not obtain rsa-key')

    def _encrypt_password(self, rsa_params: dict) -> str:
        return base64.b64encode(rsa.encrypt(self.password.encode('utf-8'), rsa_params['rsa_key']))

    def _prepare_login_request_data(self, encrypted_password: str, rsa_timestamp: str) -> dict:
        return {
            'persistence': "1",
            'encrypted_password': encrypted_password,
            'account_name': self.username,
            'encryption_timestamp': rsa_timestamp,
        }


    @staticmethod
    def _check_for_captcha(login_response: requests.Response) -> None:
        if login_response.json().get('captcha_needed', False):
            raise CaptchaRequired('Captcha required')

    def _enter_steam_guard_if_necessary(self, login_response: requests.Response) -> requests.Response:
        if login_response.json()['requires_twofactor']:
            self.one_time_code = guard.generate_one_time_code(self.shared_secret)
            return self._send_login_request()
        return login_response

    @staticmethod
    def _assert_valid_credentials(login_response: requests.Response) -> None:
        if not login_response.json()['success']:
            raise InvalidCredentials(login_response.json()['message'])

    def _perform_redirects(self, response_dict: dict) -> None:
        parameters = response_dict.get('transfer_info')
        if parameters is None:
            raise Exception('Cannot perform redirects after login, no parameters fetched')
        for pass_data in parameters:
            pass_data['params']['steamID'] = response_dict['steamID']
            self.session.post(pass_data['url'], pass_data['params'])

    def _fetch_home_page(self, session: requests.Session) -> requests.Response:
        return session.post(SteamUrl.COMMUNITY_URL + '/my/home/')

    def _update_steam_guard(self, login_response: Response) -> bool:
        client_id = login_response.json()["response"]["client_id"]
        steamid = login_response.json()["response"]["steamid"]
        request_id = login_response.json()["response"]["request_id"]
        code_type = 3
        code = guard.generate_one_time_code(self.shared_secret)

        update_data = {
            'client_id': client_id,
            'steamid': steamid,
            'code_type': code_type,
            'code': code
        }
        response = self._api_call('POST', 'IAuthenticationService', 'UpdateAuthSessionWithSteamGuardCode', params=update_data)
        if response.status_code == 200:
            self._pool_sessions_steam(client_id, request_id)
            return True
        else:
            raise Exception('Cannot update steam guard')

    def _pool_sessions_steam(self, client_id, request_id):
        pool_data = {
            'client_id': client_id,
            'request_id': request_id
        }
        response = self._api_call('POST', 'IAuthenticationService', 'PollAuthSessionStatus', params=pool_data)
        self.refresh_token = response.json()["response"]["refresh_token"]

    def _finallize_login(self):
        sessionid = self.session.cookies["sessionid"]
        redir = "https://steamcommunity.com/login/home/?goto="

        finallez_data = {
            'nonce': self.refresh_token,
            'sessionid': sessionid,
            'redir': redir
        }
        response = self.session.post("https://login.steampowered.com/jwt/finalizelogin", data = finallez_data)
        return response
