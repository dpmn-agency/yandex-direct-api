class ReportType(object):
    """
    Тип отчёта.
    https://yandex.ru/dev/direct/doc/reports/type.html
    """
    ACCOUNT_PERFORMANCE_REPORT = 'ACCOUNT_PERFORMANCE_REPORT'
    CAMPAIGN_PERFORMANCE_REPORT = 'CAMPAIGN_PERFORMANCE_REPORT'
    ADGROUP_PERFORMANCE_REPORT = 'ADGROUP_PERFORMANCE_REPORT'
    AD_PERFORMANCE_REPORT = 'AD_PERFORMANCE_REPORT'
    CRITERIA_PERFORMANCE_REPORT = 'CRITERIA_PERFORMANCE_REPORT'
    CUSTOM_REPORT = 'CUSTOM_REPORT'
    REACH_AND_FREQUENCY_PERFORMANCE_REPORT = 'REACH_AND_FREQUENCY_PERFORMANCE_REPORT'
    SEARCH_QUERY_PERFORMANCE_REPORT = 'SEARCH_QUERY_PERFORMANCE_REPORT'


class DateRangeType(object):
    """
    Период, за который требуется получить статистику в отчёте.
    Описание значений в документации: https://yandex.ru/dev/direct/doc/reports/period.html
    """
    TODAY = 'TODAY'
    YESTERDAY = 'YESTERDAY'
    LAST_3_DAYS = 'LAST_3_DAYS'
    LAST_5_DAYS = 'LAST_5_DAYS'
    LAST_7_DAYS = 'LAST_7_DAYS'
    LAST_14_DAYS = 'LAST_14_DAYS'
    LAST_30_DAYS = 'LAST_30_DAYS'
    LAST_90_DAYS = 'LAST_90_DAYS'
    LAST_365_DAYS = 'LAST_365_DAYS'
    THIS_WEEK_MON_TODAY = 'THIS_WEEK_MON_TODAY'
    THIS_WEEK_SUN_TODAY = 'THIS_WEEK_SUN_TODAY'
    LAST_WEEK = 'LAST_WEEK'
    LAST_BUSINESS_WEEK = 'LAST_BUSINESS_WEEK'
    LAST_WEEK_SUN_SAT = 'LAST_WEEK_SUN_SAT'
    THIS_MONTH = 'THIS_MONTH'
    LAST_MONTH = 'LAST_MONTH'
    ALL_TIME = 'ALL_TIME'
    CUSTOM_DATE = 'CUSTOM_DATE'
    AUTO = 'AUTO'


class ProcessingMode(object):
    """
    Режим формирования отчёта.
    Описание режимов: https://yandex.ru/dev/direct/doc/reports/mode.html
    """
    AUTO = 'auto'
    ONLINE = 'online'
    OFFLINE = 'offline'


class CampaignType(object):
    """
    Тип кампании.
    https://yandex.ru/dev/direct/doc/dg/objects/campaign.html#type
    """
    TEXT_CAMPAIGN = 'TEXT_CAMPAIGN'
    UNIFIED_CAMPAIGN = 'UNIFIED_CAMPAIGN'
    SMART_CAMPAIGN = 'SMART_CAMPAIGN'
    DYNAMIC_TEXT_CAMPAIGN = 'DYNAMIC_TEXT_CAMPAIGN'
    MOBILE_APP_CAMPAIGN = 'MOBILE_APP_CAMPAIGN'
    MCBANNER_CAMPAIGN = 'MCBANNER_CAMPAIGN'
    CPM_BANNER_CAMPAIGN = 'CPM_BANNER_CAMPAIGN'
    CPM_DEALS_CAMPAIGN = 'CPM_DEALS_CAMPAIGN'
    CPM_FRONTPAGE_CAMPAIGN = 'CPM_FRONTPAGE_CAMPAIGN'
    CPM_PRICE = 'CPM_PRICE'


class AdType(object):
    """
    Тип объявления.
    https://yandex.ru/dev/direct/doc/dg/objects/ad.html#types
    """
    TEXT_AD = 'TEXT_AD'
    SMART_AD = 'SMART_AD'
    MOBILE_APP_AD = 'MOBILE_APP_AD'
    DYNAMIC_TEXT_AD = 'DYNAMIC_TEXT_AD'
    IMAGE_AD = 'IMAGE_AD'
    CPC_VIDEO_AD = 'CPC_VIDEO_AD'
    CPM_BANNER_AD = 'CPM_BANNER_AD'
    CPM_VIDEO_AD = 'CPM_VIDEO_AD'
    SHOPPING_AD = 'SHOPPING_AD'


class CampaignState(object):
    """
    Текущее состояние показов в кампании.
    https://yandex.ru/dev/direct/doc/dg/objects/campaign.html#status
    """
    CONVERTED = 'CONVERTED'
    ARCHIVED = 'ARCHIVED'
    SUSPENDED = 'SUSPENDED'
    ENDED = 'ENDED'
    ON = 'ON'
    OFF = 'OFF'
    UNKNOWN = 'UNKNOWN'


class AdState(object):
    """
    Текущее состояние объявления.
    https://yandex.ru/dev/direct/doc/dg/objects/ad.html#state
    """
    SUSPENDED = 'SUSPENDED'
    OFF_BY_MONITORING = 'OFF_BY_MONITORING'
    ON = 'ON'
    OFF = 'OFF'
    ARCHIVED = 'ARCHIVED'


class CampaignStatusSelection(object):
    """
    Обобщённый результат модерации объектов кампании.
    https://yandex.ru/dev/direct/doc/dg/objects/campaign.html#status
    """
    DRAFT = 'DRAFT'
    MODERATION = 'MODERATION'
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'
    UNKNOWN = 'UNKNOWN'


class AdStatusSelection(object):
    """
    Результат модерации объявления.
    https://yandex.ru/dev/direct/doc/dg/objects/ad.html#status
    """
    DRAFT = 'DRAFT'
    MODERATION = 'MODERATION'
    PREACCEPTED = 'PREACCEPTED'
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'


class CampaignStatusPayment(object):
    """
    Готовность кампании к оплате.
    https://yandex.ru/dev/direct/doc/dg/objects/campaign.html#status
    """
    DISALLOWED = 'DISALLOWED'
    ALLOWED = 'ALLOWED'


class SearchStrategyPlacementTypes(object):
    """
    Места показов стратегий на поиске.
    """
    SEARCH_RESULTS = 'SearchResults'
    PRODUCT_GALLERY = 'ProductGallery'


class YesNo(object):
    YES = 'YES'
    NO = 'NO'


class ExtensionStatus(object):
    """
    Статус модерации дополнений (визитка, изображение, видеодополнение, набор быстрых ссылок).
    https://yandex.ru/dev/direct/doc/dg/objects/ad.html#addons
    """
    DRAFT = 'DRAFT'
    MODERATION = 'MODERATION'
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'
    UNKNOWN = 'UNKNOWN'
