#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import base64
import logging
from typing import Any, Iterable, List, Mapping, Optional, Set, Union

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from cached_property import cached_property
from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.adaccount import AdAccount as FBAdAccount
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User

from .base_insight_streams import AdsInsights
from .base_streams import FBMarketingIncrementalStream, FBMarketingReversedIncrementalStream, FBMarketingStream

logger = logging.getLogger("airbyte")


def fetch_thumbnail_data_url(url: str) -> Optional[str]:
    """Request thumbnail image and return it embedded into the data-link"""
    try:
        response = requests.get(url)
        if response.status_code == requests.status_codes.codes.OK:
            _type = response.headers["content-type"]
            data = base64.b64encode(response.content)
            return f"data:{_type};base64,{data.decode('ascii')}"
        else:
            logger.warning(f"Got {repr(response)} while requesting thumbnail image.")
    except requests.exceptions.RequestException as exc:
        logger.warning(f"Got {str(exc)} while requesting thumbnail image.")
    return None


class AdCreatives(FBMarketingStream):
    """AdCreative is append only stream
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-creative
    """

    entity_prefix = "adcreative"
    enable_deleted = False

    def __init__(self, fetch_thumbnail_images: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._fetch_thumbnail_images = fetch_thumbnail_images

    @cached_property
    def fields(self) -> List[str]:
        """Remove "thumbnail_data_url" field because it is computed field and it's not a field that we can request from Facebook"""
        return [f for f in super().fields if f != "thumbnail_data_url"]

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Read with super method and append thumbnail_data_url if enabled"""
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            if self._fetch_thumbnail_images:
                thumbnail_url = record.get("thumbnail_url")
                if thumbnail_url:
                    record["thumbnail_data_url"] = fetch_thumbnail_data_url(thumbnail_url)
            yield record

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_creatives(params=params)


class CustomConversions(FBMarketingStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/custom-conversion"""

    entity_prefix = "customconversion"
    enable_deleted = False

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_custom_conversions(params=params)


class Ads(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup"""

    entity_prefix = "ad"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ads(params=params)


class AdSets(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign"""

    entity_prefix = "adset"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_sets(params=params)


class Campaigns(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group"""

    entity_prefix = "campaign"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_campaigns(params=params)


class Activities(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-activity"""

    entity_prefix = "activity"
    cursor_field = "event_time"
    primary_key = None

    def list_objects(self, fields: List[str], params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_activities(fields=fields, params=params)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Main read method used by CDK"""
        loaded_records_iter = self.list_objects(fields=self.fields, params=self.request_params(stream_state=stream_state))

        for record in loaded_records_iter:
            if isinstance(record, AbstractObject):
                yield record.export_all_data()  # convert FB object to dict
            else:
                yield record  # execute_in_batch will emmit dicts

    def _state_filter(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Additional filters associated with state if any set"""
        state_value = stream_state.get(self.cursor_field)
        since = self._start_date if not state_value else pendulum.parse(state_value)

        potentially_new_records_in_the_past = self._include_deleted and not stream_state.get("include_deleted", False)
        if potentially_new_records_in_the_past:
            self.logger.info(f"Ignoring bookmark for {self.name} because of enabled `include_deleted` option")
            since = self._start_date

        return {"since": since.int_timestamp}


class Videos(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/video"""

    entity_prefix = "video"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        # Remove filtering as it is not working for this stream since 2023-01-13
        return self._api.account.get_ad_videos(params=params, fields=self.fields)


class AdAccount(FBMarketingStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-account"""

    use_batch = False
    enable_deleted = False

    def get_task_permissions(self) -> Set[str]:
        """https://developers.facebook.com/docs/marketing-api/reference/ad-account/assigned_users/"""
        res = set()
        me = User(fbid="me", api=self._api.api)
        for business_user in me.get_business_users():
            assigned_users = self._api.account.get_assigned_users(params={"business": business_user["business"].get_id()})
            for assigned_user in assigned_users:
                if business_user.get_id() == assigned_user.get_id():
                    res.update(set(assigned_user["tasks"]))
        return res

    @cached_property
    def fields(self) -> List[str]:
        properties = super().fields
        # https://developers.facebook.com/docs/marketing-apis/guides/javascript-ads-dialog-for-payments/
        # To access "funding_source_details", the user making the API call must have a MANAGE task permission for
        # that specific ad account.
        if "funding_source_details" in properties and "MANAGE" not in self.get_task_permissions():
            properties.remove("funding_source_details")
        if "is_prepay_account" in properties and "MANAGE" not in self.get_task_permissions():
            properties.remove("is_prepay_account")
        return properties

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        """noop in case of AdAccount"""
        return [FBAdAccount(self._api.account.get_id())]


class Images(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-image"""

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_images(params=params, fields=self.fields)

    def get_record_deleted_status(self, record) -> bool:
        return record[AdImage.Field.status] == AdImage.Status.deleted


class AdsInsightsCountry(AdsInsights):
    breakdowns = ["country"]


class AdsInsightsRegion(AdsInsights):
    breakdowns = ["region"]


class AdsInsightsDma(AdsInsights):
    breakdowns = ["dma"]


class AdsInsightsActionCarouselCard(AdsInsights):
    action_breakdowns = ["action_carousel_card_id", "action_carousel_card_name"]


class AdsInsightsActionConversionDevice(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsActionProductID(AdsInsights):
    breakdowns = ["product_id"]
    action_breakdowns = []


class AdsInsightsActionReaction(AdsInsights):
    action_breakdowns = ["action_reaction"]


class AdsInsightsActionVideoSound(AdsInsights):
    action_breakdowns = ["action_video_sound"]


class AdsInsightsActionVideoType(AdsInsights):
    action_breakdowns = ["action_video_type"]


# Ad Insights with Breakdowns 
class AdsInsightsActionType(AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]
    
class AdsInsightsAge(AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]

class AdsInsightsGender(AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]

class AdsInsightsAgeAndGender(AdsInsights):
    breakdowns = ["age", "gender"]
    action_breakdowns = ["action_type"]

class AdsInsightsCountry(AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]

class AdsInsightsRegion(AdsInsights):
    breakdowns = ["region"]
    action_breakdowns = ["action_type"]

class AdsInsightsDMAActionTypeActionType(AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]
   
class AdsInsightsImpressionDevice(AdsInsights):
    breakdowns = ["impression_device"]
    action_breakdowns = ["action_type"]

class AdsInsightsDevicePlatform(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]

class AdsInsightsPublisherPlatform(AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]

class AdsInsightsPublisherAndDevicePlatform(AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]

class AdsInsightsImpressionDeviceAndDevicePlatform(AdsInsights):
    breakdowns = ["impression_device", "device_platform"]
    action_breakdowns = ["action_type"]
    
class AdsInsightsPublisherPlatformAndImpressionDevice(AdsInsights):
    breakdowns = ["publisher_platform", "impression_device"]
    action_breakdowns = ["action_type"]

class AdsInsightsPlatformAndDevice(AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]

# Adset insights with Breakdowns

class MixingAdsetPrimaryKey:
    
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """Build complex PK based on slices and breakdowns"""
        return ["date_start", "account_id", "adset_id"] + self.breakdowns

class AdsetInsightsActionType(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]
    schema = "adset_insights"
    
    
class AdsetInsightsAge(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsGender(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsAgeAndGender(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["age", "gender"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsCountry(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsRegion(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["region"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsDMAActionType(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"

   
class AdsetInsightsImpressionDevice(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["impression_device"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsDevicePlatform(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsPublisherPlatform(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsPublisherAndDevicePlatform(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsImpressionDeviceAndDevicePlatform(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["impression_device", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"

  
class AdsetInsightsPublisherPlatformAndImpressionDevice(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "impression_device"]
    action_breakdowns = ["action_type"]
    schema = "adset_insights"


class AdsetInsightsPlatformAndDevice(MixingAdsetPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]   
    schema = "adset_insights" 

# Campaign Insights with Breakdowns 

class MixingCampaignPrimaryKey:
    
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """Build complex PK based on slices and breakdowns"""
        return ["date_start", "account_id", "campaign_id"] + self.breakdowns


class CampaignInsightsActionType(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"

   
class CampaignInsightsAge(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsGender(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsAgeAndGender(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["age", "gender"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsCountry(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsRegion(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["region"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsDMAActionType(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"

   
class CampaignInsightsImpressionDevice(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["impression_device"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsDevicePlatform(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsPublisherPlatform(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsPublisherAndDevicePlatform(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsImpressionDeviceAndDevicePlatform(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["impression_device", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"

    
class CampaignInsightsPublisherPlatformAndImpressionDevice(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "impression_device"]
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"


class CampaignInsightsPlatformAndDevice(MixingCampaignPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]
    schema = "campaign_insights"

    
# Adaccount insights with breakdowns 

class MixingAdAccountPrimaryKey:
    
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """Build complex PK based on slices and breakdowns"""
        return ["date_start", "account_id"] + self.breakdowns


class AdaccountInsightsActionType(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"
 
    
class AdaccountInsightsAge(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsGender(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsAgeAndGender(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["age", "gender"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsCountry(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsRegion(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["region"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsDMAActionType(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsImpressionDevice(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["impression_device"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsDevicePlatform(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsPublisherPlatform(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsPublisherAndDevicePlatform(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsImpressionDeviceAndDevicePlatform(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["impression_device", "device_platform"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"
 
    
class AdaccountInsightsPublisherPlatformAndImpressionDevice(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "impression_device"]
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"


class AdaccountInsightsPlatformAndDevice(MixingAdAccountPrimaryKey, AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]
    schema = "adacount_insights"