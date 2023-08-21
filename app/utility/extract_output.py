import csv
import os
from datetime import datetime
import configure as output_header


def findmin(effect_date, time_list):
    fmt = '%Y-%m-%dT%H:%M:%S'
    effect_fmt = datetime.strptime(effect_date, fmt)
    timediff_list = []
    for x in time_list:
        x_timestamp = datetime.strptime(x, fmt)
        td = x_timestamp - effect_fmt
        td_secs = int(round(td.total_seconds()))
        timediff_list.append(td_secs)

    m = min(i for i in timediff_list if i > 0)
    min_index = timediff_list.index(m)

    return min_index


def write_output(header_col, resp_list, file_name, msisdn, trxid):
    check_file = os.path.exists(file_name)

    if len(resp_list) == 0:
        resp = ['' for x in header_col]
        resp[0] = msisdn
        resp[1] = trxid
        resp_list.append(resp)

    if not check_file:
        with open(file_name, 'w', newline="\n") as f:
        # with open(file_name, 'w', encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerow(header_col)
            write.writerows(resp_list)
    else:
        # with open(file_name, 'a', encoding='utf-8') as f:
        with open(file_name, 'a', newline="\n") as f:
            write = csv.writer(f)
            write.writerows(resp_list)
    pass


def extract_outbound(raw_data, msisdn, trxid, file_name):
    resp_list = []
    seq = 0
    try:
        data_list = [i for i in raw_data['GetCampaignHistoryResponse']['UseCaseRun'] if
                     i.get('ucName') and i['ucName'] != ""]
    except:
        data_list = []

    for i in data_list:
        try:
            campaign_id = i['id']
        except KeyError:
            campaign_id = ""
        try:
            campaign_name = i['ucName']
        except KeyError:
            campaign_name = ""
        step2_list, step3_list, step4_list, step5_list = [], [], [], []

        for j in i['Event']:
            try:
                event_type = j['Type']
            except KeyError:
                event_type = ""

            try:
                reward_type = j['Parameters']['RewardType']
            except KeyError:
                reward_type = ""

            try:
                event_status = j['Status']
            except KeyError:
                event_status = ""

            if event_type == "Reward Given" and reward_type == "Offer Made" \
                    and event_status == "Success":
                step2_list.append(j)

            if event_type == "Notification Report":
                step3_list.append(j)

            if event_type == "Offer Provisioning Status Received" and event_status == "Success":
                step4_list.append(j)

            if event_type == "Reward Given" and reward_type == "Offer Accepted":
                step5_list.append(j)

        if len(step2_list) > 0:
            seq += 1
            for idx, row in enumerate(step2_list):
                offerExpirationDate = row['Parameters']['Product'][0]['ExpirationTime']
                offerEffectiveDate = row['Time']
                offer_name = row['Parameters']['OfferName']
                max_data = row['Parameters']['Product'][0]
                offer_code = max_data['ProductOfferingID']
                amount = [x['Value'] for x in max_data['Parameter'] if x['Name'] == 'Package Fee Inc VAT Satang'][0]
                _validity = [x['Value'] for x in max_data['Parameter'] if x['Name'] == 'Package Validity Hours'][0]
                package_id = [x['Value'] for x in max_data['Parameter'] if x['Name'] == 'Package ID'][0]
                package_desc = [x['Value'] for x in max_data['Parameter'] if x['Name'] == 'Package Name'][0]

                try:
                    amount = int(amount) / 100
                except:
                    amount = 0

                contactDate, fromChannel, offerMessage, fulfillmentDate, responseStatus = '', '', '', '', ''

                if len(step3_list) > 0:
                    step3_time_list = [x['Time'] for x in step3_list]
                    min3_index = findmin(offerEffectiveDate, step3_time_list)
                    contactDate = step3_list[min3_index]['Time']
                    fromChannel = step3_list[min3_index]['Parameters']['DeliveryChannel']
                    offerMessage = step3_list[min3_index]['Parameters']['Text']

                if len(step4_list) > 0:
                    step4_time_list = [x['Time'] for x in step4_list]
                    min4_index = findmin(offerEffectiveDate, step4_time_list)
                    fulfillmentDate = step4_list[min4_index]['Time']

                if len(step5_list) > 0:
                    step5_time_list = [x['Time'] for x in step5_list]
                    min5_index = findmin(offerEffectiveDate, step5_time_list)
                    RewardType = step5_list[min5_index]['Parameters']['RewardType']
                    responseStatus = "Response"
                else:
                    responseStatus = "Contacted"

                order = seq if len(step2_list) == 1 else "{}_{}".format(seq, idx + 1)
                resp = msisdn, trxid, campaign_id, campaign_name, order, offer_name, offerEffectiveDate, offer_code, \
                       offerExpirationDate, amount, _validity, package_id, package_desc, contactDate, fromChannel, \
                       offerMessage, fulfillmentDate, responseStatus
                resp_list.append(resp)

    write_output(output_header.outbound_header, resp_list, file_name, msisdn, trxid)


def extract_inbound(raw_data, msisdn, trxid, file_name):
    resp_list = []

    try:
        json_data = raw_data['GetAvailableOffersResponse']
    except:
        json_data = {}

    for i in json_data:
        if i == "channelId":
            channel_id = json_data[i].split("|")
            application = channel_id[1].split(":")[1]
            zone = channel_id[2].split(":")[1]

        if i == "offer" and json_data[i]:
            for idx, j in enumerate(json_data[i]):
                package_type = ""
                score = ""

                _type = [x['value'] for x in j['productOffering']['poAttribute'] if x['name'] == "Package Type"]
                _score = [x['value'] for x in j['productOffering']['poAttribute'] if x['name'] == "Score"]
                if _score:
                    score = _score[0]

                if _type:
                    package_type = _type[0]

                resp = msisdn, trxid, application, zone, idx + 1, j['campaignId'], j['campaignPriority'], j[
                    'issuedTime'], j['offerDescription'], j['productOffering']['name'], j['productOffering'][
                           'offeredPOId'], j['productOffering']['poAttribute'][0]['value'], package_type, score, \
                       j['productOffering']['recoOffersStrategy']['category'][0], \
                       j['productOffering']['recoOffersStrategy']['id'], j['productOffering']['recoOffersStrategy'][
                           'name'], j['shortDescription']
                resp_list.append(resp)

    write_output(output_header.inbound_header, resp_list, file_name, msisdn, trxid)


def extract_nba(raw_data, msisdn, trxid, file_name):
    resp_list = []

    try:
        json_data = raw_data['GetNBAStatesResponse']
    except:
        json_data = {}

    for i in json_data:
        if i == "Description":
            descript = json_data[i]

        if i == 'ResponseTimestamp':
            timestamp = json_data[i]

        if i == "Result":
            for idx, j in enumerate(json_data[i]):
                if j['Parameters']:
                    for count, x in enumerate(j['Parameters']):
                        seq = "{}_{}".format(idx + 1, count + 1)
                        resp = msisdn, trxid, timestamp, descript, seq, j['Shelf'], x['Priority'], x['State'], x[
                            'SubState']
                        resp_list.append(resp)

    write_output(output_header.nba_header, resp_list, file_name, msisdn, trxid)


