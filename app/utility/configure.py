output_folder = "output"
key_sign = "%"
config_folder = "request_template"
headers = {'Content-Type': 'application/json'}
delay_time = 100    #millisecs
special_params = {
    "param_msisdn": "msisdn",
    "param_eventId": "eventid",
    "param_accountType": "accounttype",
    "param_parameter": "param",
    "param_context": "context",
    "param_date": "date",
    "param_current": "currenttime"
}

outbound_header = ["MSISDN", "trxid", "CampaignCode", "CampaignName", "Seq",
                   "offerName", "offerEffectiveDate", "offerCode",
                   "offerExpirationDate", "amount",
                   "validity", "packageCode", "packageDescription", "contactDate", "fromChannel", "offerMessage",
                   "fulfillmentDate", "responseStatus"
                   ]

inbound_header = ["MSISDN", "trxid", "APPLICATION", "ZONE", "seq", "offer.campaignId", "offer.campaignPriority",
                  "offer.issuedTime", "offer.offerDescription", "offer.name", "offer.offeredPOId",
                  "offer.poAttribute", "Package Type",
                  "Score", "OfferCat", "strategyID", "strategyName", "shortDescription"]

nba_header = ["MSISDN", "trxid", "Timestamp", "Description", "seq", "Shelf", "Priority", "State", "SubState"]

