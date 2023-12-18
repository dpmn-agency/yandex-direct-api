import requests
from requests import request as http_request
from requests.exceptions import ConnectionError
from time import sleep

from yandex_direct_api.constants import DateRangeType, ProcessingMode
from yandex_direct_api.exceptions import YandexDirectApiError, YandexDirectClientError


class YandexDirect:
    def __init__(self, app_token: str, sandbox: bool = False):
        self.__app_token = app_token
        self._api_endpoint = f'https://{"api" if sandbox is False else "api-sandbox"}.direct.yandex.com'

    def _make_request(self, service: str, headers: dict, body: dict) -> requests.Response:
        """
        Общая функция отправки запросов к API.
        :param service: Название сервиса API в нижнем регистре. Список сервисов: https://yandex.ru/dev/direct/doc/ref-v5/concepts/about.html
        :param headers: Заголовки запроса.
        :param body: Тело запроса.
        :return:
        """
        api_url = '/'.join([self._api_endpoint, 'json', 'v5', service])
        headers.update({
            'Authorization': f'Bearer {self.__app_token}'
        })

        while True:
            try:
                response = http_request('POST', url=api_url, headers=headers, json=body)
                # Принудительная обработка ответа в кодировке UTF-8
                response.encoding = 'utf-8'

                if response.status_code == 200:
                    return response
                elif response.status_code in (201, 202):
                    retry_in = int(response.headers.get('retryIn', 60))
                    sleep(retry_in)
                else:
                    raise YandexDirectApiError(response.text)
            except ConnectionError:
                raise YandexDirectClientError(ConnectionError)

    def get_report(self, report_name: str, report_type: str, field_names: list, mode='json', **kwargs):
        """
        Формирование отчёта из Yandex.Direct.
        :param report_name: Произвольное уникальное название отчёта.
        :param report_type: Тип отчёта https://yandex.ru/dev/direct/doc/reports/type.html.
        :param field_names: Имена полей (столбцов), которые будут присутствовать в отчёте.
        :param mode: Формат отчёта. Можно указать значения 'tsv' или 'json'.
        :param kwargs: Другие параметры (https://yandex.ru/dev/direct/doc/reports/spec.html) отчёта и заголовки (https://yandex.ru/dev/direct/doc/reports/headers.html).
        :return: Содержание отчёта https://yandex.ru/dev/direct/doc/reports/report-format.html.
        """
        # Параметры заголовков запроса
        skip_report_header = kwargs.get('skip_report_header', True)
        skip_column_header = kwargs.get('skip_column_header', True)
        skip_report_summary = kwargs.get('skip_report_summary', True)

        headers = {
            'Client-Login': kwargs.get('client_login', None),
            'processingMode': kwargs.get('processing_mode', ProcessingMode.AUTO),
            'returnMoneyInMicros': str(kwargs.get('return_money_in_micros', False)).lower(),
            'skipReportHeader': str(skip_report_header).lower(),
            'skipColumnHeader': str(skip_column_header).lower(),
            'skipReportSummary': str(skip_report_summary).lower()
        }

        body = {
            'params': {
                'SelectionCriteria': {
                    'Filter': kwargs.get('filter', [])
                },
                'Goals': kwargs.get('goals', []),
                'AttributionModels': kwargs.get('attribution_models', []),
                'FieldNames': field_names,
                'Page': {
                    'Limit': kwargs.get('page_limit', 1_000_000),
                    'Offset': kwargs.get('page_offset', 0)
                },
                'OrderBy': kwargs.get('order_by', []),
                'ReportName': report_name,
                'ReportType': report_type,
                'DateRangeType': kwargs.get('date_range_type', DateRangeType.AUTO),
                'Format': 'TSV',
                'IncludeVAT': 'NO' if kwargs.get('include_vat', False) is False else 'YES'
            }
        }

        # Параметры DateFrom и DateTo обязательны при значении CUSTOM_DATE параметра DateRangeType и недопустимы
        # при других значениях.
        if body['params']['DateRangeType'] == DateRangeType.CUSTOM_DATE:
            try:
                body['params']['SelectionCriteria']['DateFrom'] = kwargs['date_from']
                body['params']['SelectionCriteria']['DateTo'] = kwargs['date_to']
            except KeyError:
                YandexDirectClientError(KeyError)

        response = self._make_request('reports', headers, body)

        if mode.lower() == 'json':
            # Преобразование отчёта к JSON формату
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
        elif mode.lower() == 'tsv':
            return response.text
        else:
            raise YandexDirectClientError(f"""
                '{mode}' не является допустимым значением параметра mode. Разрешённые значения: json, tsv.
            """)

    def get_campaigns(self, field_names: list, **kwargs):
        """
        Возвращает параметры кампаний, отвечающих заданным критериям.
        :param field_names: Имена параметров, общие для всех типов кампаний, которые требуется получить.
        :param kwargs: Другие параметры запроса согласно схеме https://yandex.ru/dev/direct/doc/ref-v5/campaigns/get.html.
        :return: Структура ответа https://yandex.ru/dev/direct/doc/ref-v5/campaigns/get.html#output.
        """
        headers = {
            'Client-Login': kwargs.get('client_login', None)
        }

        # Параметры тела запроса
        page_limit = kwargs.get('page_limit', 10000)

        body = {
            'method': 'get',
            'params': {
                'SelectionCriteria': {
                    'Ids': kwargs.get('ids', []),
                    'Types': kwargs.get('types', []),
                    'States': kwargs.get('states', []),
                    'Statuses': kwargs.get('statuses', []),
                    'StatusesPayment': kwargs.get('statuses_payment', [])
                },
                'FieldNames': field_names,
                'TextCampaignFieldNames': kwargs.get('text_campaign_field_names', []),
                'TextCampaignSearchStrategyPlacementTypesFieldNames': kwargs.get('text_campaign_search_strategy_placement_types_field_names', []),
                'MobileAppCampaignFieldNames': kwargs.get('mobile_app_campaign_field_names', []),
                'DynamicTextCampaignFieldNames': kwargs.get('dynamic_text_campaign_field_names', []),
                'CpmBannerCampaignFieldNames': kwargs.get('cpm_banner_campaign_field_names', []),
                'SmartCampaignFieldNames': kwargs.get('smart_campaign_field_names', []),
                'UnifiedCampaignFieldNames': kwargs.get('unified_campaign_field_names', []),
                'UnifiedCampaignSearchStrategyPlacementTypesFieldNames': kwargs.get('unified_campaign_search_strategy_placement_types_field_names', []),
                'Page': {
                    'Limit': page_limit if page_limit <= 10000 else 10000,
                    'Offset': kwargs.get('page_offset', 0)
                }
            }
        }

        response = self._make_request('campaigns', headers, body)
        return response.json()

    def get_ads(self, field_names: list, **kwargs) -> dict:
        """
        Возвращает параметры объявлений, отвечающих заданным критериям.
        :param field_names: Имена параметров верхнего уровня, которые требуется получить.
        :param kwargs: Другие параметры запроса согласно схеме https://yandex.ru/dev/direct/doc/ref-v5/ads/get.html.
        :return: Структура ответа https://yandex.ru/dev/direct/doc/ref-v5/ads/get.html#output.
        """
        headers = {
            'Client-Login': kwargs.get('client_login', None)
        }

        # Параметры тела запроса
        mobile = kwargs.get('mobile', None)
        ids = kwargs.get('ids', [])
        ad_group_ids = kwargs.get('ad_group_ids', [])
        campaign_ids = kwargs.get('campaign_ids', [])
        page_limit = kwargs.get('page_limit', 10000)

        if any((ids, ad_group_ids, campaign_ids)) is False:
            raise YandexDirectClientError('Должен быть указан хотя бы один из параметров: Ids, AdGroupIds, CampaignIds.')

        body = {
            'method': 'get',
            'params': {
                'SelectionCriteria': {
                    'Ids': ids[:10000],
                    'AdGroupIds': ad_group_ids[:1000],
                    'CampaignIds': campaign_ids[:10],
                    'Types': kwargs.get('types', []),
                    'States': kwargs.get('states', []),
                    'Statuses': kwargs.get('statuses', []),
                    'VCardIds': kwargs.get('vcard_ids', [])[:50],
                    'SitelinkSetIds': kwargs.get('sitelink_set_ids', [])[:50],
                    'AdImageHashes': kwargs.get('ad_image_hashes', [])[:50],
                    'VCardModerationStatuses': kwargs.get('vcard_moderation_statuses', []),
                    'SitelinksModerationStatuses': kwargs.get('sitelinks_moderation_statuses', []),
                    'AdImageModerationStatuses': kwargs.get('ad_image_moderation_statuses', []),
                    'AdExtensionIds': kwargs.get('ad_extension_ids', [])[:50]
                },
                'FieldNames': field_names,
                'TextAdFieldNames': kwargs.get('text_ad_field_names', []),
                'TextAdPriceExtensionFieldNames': kwargs.get('text_ad_price_extension_field_names', []),
                'MobileAppAdFieldNames': kwargs.get('mobile_app_ad_field_names', []),
                'DynamicTextAdFieldNames': kwargs.get('dynamic_text_ad_field_names', []),
                'TextImageAdFieldNames': kwargs.get('text_image_ad_field_names', []),
                'MobileAppImageAdFieldNames': kwargs.get('mobile_app_image_ad_field_names', []),
                'TextAdBuilderAdFieldNames': kwargs.get('text_ad_builder_ad_field_names', []),
                'MobileAppAdBuilderAdFieldNames': kwargs.get('mobile_app_ad_builder_ad_field_names', []),
                'MobileAppCpcVideoAdBuilderAdFieldNames': kwargs.get('mobile_app_cpc_video_ad_builder_ad_field_names', []),
                'CpcVideoAdBuilderAdFieldNames': kwargs.get('cpc_video_ad_builder_ad_field_names', []),
                'CpmBannerAdBuilderAdFieldNames': kwargs.get('cpm_banner_ad_builder_ad_field_names', []),
                'CpmVideoAdBuilderAdFieldNames': kwargs.get('cpm_video_ad_builder_ad_field_names', []),
                'SmartAdBuilderAdFieldNames': kwargs.get('smart_ad_builder_ad_field_names', []),
                'ShoppingAdFieldNames': kwargs.get('shopping_ad_field_names', []),
                'Page': {
                    'Limit': page_limit if page_limit <= 10000 else 10000,
                    'Offset': kwargs.get('page_offset', 0)
                }
            }
        }

        if mobile is not None:
            body['params']['SelectionCriteria']['Mobile'] = mobile

        response = self._make_request('ads', headers, body)
        return response.json()
