import requests
import time


class YandexDirect:
    def __init__(self, app_token: str, sandbox: bool = False):
        self._app_token = app_token
        self.sandbox = sandbox

    # Общая функция отправки запросов к API
    # :service: <str> Название сервиса API в нижнем регистре.
    # Список сервисов: https://yandex.ru/dev/direct/doc/ref-v5/concepts/about.html
    # :headers: <dict> Заголовки запроса.
    # :body: <dict> Тело запроса.
    # :attempts: <int> Количество попыток получения офлайн-отчёта.
    def _send_request(self, service: str, headers: dict, body: dict, attempts: int = 5):
        if self.sandbox is False:
            api_endpoint = 'https://api.direct.yandex.com'
        else:
            api_endpoint = 'https://api-sandbox.direct.yandex.com'

        api_url = '/'.join([api_endpoint, 'json', 'v5', service])
        headers.update({
            'Authorization': f'Bearer {self._app_token}'
        })

        response = requests.request('POST', url=api_url, headers=headers, json=body)
        response.encoding = 'utf-8'

        if response.status_code in (201, 202):
            if attempts > 0:
                retry_in = int(response.headers.get('retryIn', 60))
                attempts -= 1
                time.sleep(retry_in)

                return self._send_request(service, headers, body, attempts)

        return response

    # Метод для получения статистики по аккаунту
    # Спецификация отчёта: https://yandex.ru/dev/direct/doc/reports/spec.html
    # Описание заголовков запроса: https://yandex.ru/dev/direct/doc/reports/headers.html
    def reports(self,
                report_name: str,
                report_type: str,
                report_fields: list,
                date_range_type: str = 'YESTERDAY',
                date_from: str = None,
                date_to: str = None,
                data_filter: list = None,
                goals: list = None,
                attribution_models: list = None,
                page_limit: int = 1000000,
                order_by: list = None,
                include_vat: bool = False,
                client_login: str = None,
                processing_mode: str = 'auto',
                return_money_in_micros: bool = True,
                skip_report_header: bool = True,
                skip_column_header: bool = True,
                skip_report_summary: bool = True):

        headers = {
            'Client-Login': client_login,
            'processingMode': processing_mode,
            'returnMoneyInMicros': str(return_money_in_micros).lower(),
            'skipReportHeader': str(skip_report_header).lower(),
            'skipColumnHeader': str(skip_column_header).lower(),
            'skipReportSummary': str(skip_report_summary).lower()
        }

        if data_filter is None:
            data_filter = []

        if goals is None:
            goals = []

        if attribution_models is None:
            attribution_models = []

        if order_by is None:
            order_by = []

        body = {
            'params': {
                'SelectionCriteria': {
                    'DateFrom': '',
                    'DateTo': '',
                    'Filter': data_filter
                },
                'Goals': goals,
                'AttributionModels': attribution_models,
                'FieldNames': report_fields,
                'Page': {
                    'Limit': page_limit
                },
                'OrderBy': order_by,
                'ReportName': report_name,
                'ReportType': report_type,
                'DateRangeType': date_range_type,
                'Format': 'TSV',
                'IncludeVAT': 'NO' if include_vat is False else 'YES'
            }
        }

        # Параметры DateFrom и DateTo обязательны при значении CUSTOM_DATE параметра DateRangeType и недопустимы
        # при других значениях.
        if date_from and date_to:
            body['params']['SelectionCriteria']['DateFrom'] = date_from
            body['params']['SelectionCriteria']['DateTo'] = date_to
        else:
            body['params']['SelectionCriteria'].pop('DateFrom')
            body['params']['SelectionCriteria'].pop('DateTo')

        response = self._send_request('reports', headers, body)

        if response.status_code == 200:
            data_rows = response.text.strip().split('\n')
            data_headers = []

            # Преобразование объекта в зависимости от наличия названий отчёта и колонок
            if skip_report_header is False:
                data_rows.pop(0)

            if skip_column_header is False:
                data_headers = data_rows.pop(0).split('\t')

            if skip_report_summary is False:
                data_rows.pop(-1)

            # Формирование ответа
            if len(data_headers) > 0:
                data = [dict(zip(data_headers, row.split('\t'))) for row in data_rows]
            else:
                data = [row.split('\t') for row in data_rows]

            return data
        else:
            print(response.json())
            return response.json()

    # Возвращает список рекламодателей — клиентов агентства, их параметры и настройки главных
    # представителей рекламодателя.
    # Документация метода: https://yandex.ru/dev/direct/doc/ref-v5/agencyclients/get.html
    def agency_clients(self,
                       field_names: list,
                       logins: list = None,
                       archived: bool = False,
                       limit: int = 10000,
                       offset: int = 0):
        body = {
            'method': 'get',
            'params': {
                'SelectionCriteria': {
                    'Logins': [] if logins is None else logins,
                    'Archived': 'NO' if archived is False else 'YES'
                },
                'FieldNames': field_names,
                'Page': {
                    'Limit': limit,
                    'Offset': offset
                }
            }
        }

        response = self._send_request('agencyclients', headers={}, body=body)

        return response.json()

    def campaigns(self, client_login: str = None):
        headers = {
            'Client-Login': client_login
        }

        body = {
            'method': 'get',
            'params': {
                'SelectionCriteria': {},
                'FieldNames': ['Id', 'Name']
            }
        }

        response = self._send_request('campaigns', headers=headers, body=body)

        return response.json()

    def ads(self, campaign_ids: list, client_login: str = None):
        headers = {
            'Client-Login': client_login
        }

        body = {
            'method': 'get',
            'params': {
                'SelectionCriteria': {
                    'CampaignIds': campaign_ids
                },
                'FieldNames': ['CampaignId', 'Id'],
                'TextAdFieldNames': ['Href']
            }
        }

        response = self._send_request('ads', headers=headers, body=body)

        return response.json()
