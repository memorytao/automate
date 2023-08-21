import csv
import json
import os.path
import time
from datetime import datetime, date, timedelta
import requests
import sys
import re

sys.path.append('utility')
from extract_output import extract_nba, extract_outbound, extract_inbound
import configure as conf


requests.packages.urllib3.disable_warnings()

# special_params = {
#     "param_msisdn": "msisdn",
#     "param_eventId": "eventid",
#     "param_accountType": "accounttype",
#     "param_parameter": "param",
#     "param_context": "context",
#     "param_date": "date",
#     "param_current": "currenttime"
# }


def extract_data(row_data, csv_header):
    msisdn = row_data[csv_header["msisdn"]].strip().replace("'", "")
    action = row_data[csv_header["action"]].strip().replace("'", "")
    eventId = row_data[csv_header["eventId".lower()]].strip().replace("'", "")
    accountType = row_data[csv_header["accounttype"]].replace("'", "")
    body = row_data[csv_header["body"]]
    parameter = row_data[csv_header["parameter"]]
    context = row_data[csv_header["context"]]

    csv_body = {}
    csv_param = []
    csv_context = []

    if body.strip() != "":
        for y in body.strip().split(";"):
            if y != "":
                y = y.strip()
                y_key = y.split(":")[0].replace('\"', '').replace("\'", "")
                idx_sign = y.find(":")
                y_val = y[idx_sign + 1:].strip()

                if y_val and (y_val[0] == y_val[-1] == '\'') or (y_val[0] == y_val[-1] == '"'):
                    y_val = y_val[1:-1]

                csv_body[y_key] = y_val

    if parameter.strip() != "":
        for y in parameter.strip().split(";"):
            ext_val = y.split(":")
            idx_sign = y.find(":")
            y_key = ext_val[0].replace('"', '').replace("'", '')
            y_val = y[idx_sign + 1:].replace('"', '').replace("'", '').strip()

            if re.findall('{{add.*days}}', y_val):
                time_val = y_val.replace('{{add', '').replace('days}}', '').lower()

                try:
                    today = date.today() + timedelta(days=int(time_val))
                except:
                    today = ""

                if today != "":
                    y_val = today.strftime("%m/%d/%Y 00:00:00")

            csv_param.append({"ParameterType": y_key,
                              "ParameterValue": y_val})
        csv_param = json.dumps(csv_param)

    if context.strip() != "":
        for y in context.strip().split(";"):
            ext_val = y.split(":")
            idx_sign = y.find(":")
            y_key = ext_val[0].replace('"', '').replace("'", '')
            y_val = y[idx_sign + 1:].replace('"', '').replace("'", '').strip()

            if re.findall('{{add.*days}}', y_val):
                time_val = y_val.replace('{{add', '').replace('days}}', '').lower()
                try:
                    today = date.today() + timedelta(days=int(time_val))
                except:
                    today = ""

                if today != "":
                    y_val = today.strftime("%m/%d/%Y 00:00:00")

            csv_context.append({"contextAttributeType": y_key,
                                "contextAttributeValue": y_val})
        csv_context = json.dumps(csv_context)

    try:
        with open("{}/{}.json".format(conf.config_folder, action)) as conf_file:

            data = conf_file.read()
            conf_file.close()
            str_list = re.findall('"{{.*}}"', data)
            int_list = re.findall('{{.*}}', data)
            rm_list = [x.replace('"', '') for x in str_list]
            val_list = list(set(str_list + int_list) - set(rm_list))

            for val in val_list:
                new_val = ""
                csv_var = val.replace("{{", '').replace("}}", '').replace('"', '')

                if csv_var in csv_body:
                    new_val = csv_body[csv_var].strip()
                elif csv_var.lower() == conf.special_params['param_msisdn']:
                    new_val = msisdn
                elif csv_var.lower() == conf.special_params['param_eventId']:
                    new_val = eventId
                elif csv_var.lower() == conf.special_params['param_accountType']:
                    new_val == accountType
                elif csv_var.lower() == conf.special_params['param_parameter']:
                    new_val = csv_param
                elif csv_var.lower() == conf.special_params['param_context']:
                    new_val = csv_context
                elif csv_var.lower() == conf.special_params['param_date']:
                    new_val = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                elif csv_var.lower() == conf.special_params['param_current']:
                    new_val = (datetime.now() + timedelta(milliseconds=120000)).strftime("%Y%m%d%H%M%S")
                else:
                    new_val = '"{{' + csv_var + '}}"'

                ignore_list = ['param', 'context']
                if new_val != "":
                    if ('"' in val or "'" in val) and csv_var.lower() not in ignore_list:
                        new_val = '"{}"'.format(new_val)
                    data = data.replace(val, str(new_val))

            # print(data)
            resp = json.loads(data)
            resp['STATUS'] = "Success"
            return resp
    except Exception as e:
        print(str(e))
        return {"STATUS": str(e)}


def automate_app(input_file):
    check_file = os.path.exists(input_file)
    resp_list = []
    outbound_count, inbound_count, nba_count = 0, 0, 0

    if not os.path.exists(conf.output_folder):
        os.makedirs(conf.output_folder)
        print("output folder is created!")

    if not check_file:
        print("Cannot find file {}, please check again".format(input_file))
        exit()

    file_name = os.path.basename(input_file)
    with open(input_file, encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_header = {}
        header_col = []
        for idx, row in enumerate(csv_reader):
            if idx == 0:
                for i, x in enumerate(row):
                    if x.lower().strip() != "":
                        csv_header[x.lower()] = i
                header_col = row
            elif len(row) != 0:
                for i in range(len(row), len(csv_header)):
                    row.append(",")

                col_result = csv_header['result']
                col_trxid = csv_header['trxid']
                col_detail = csv_header['detail']

                payload = extract_data(row, csv_header)
                msisdn = row[csv_header["msisdn"]]
                trxid = str(int(time.time() * 100000))

                row[col_result] = ''
                row[col_trxid] = trxid
                row[col_detail] = ''

                if payload['STATUS'] == 'Success':
                    req_url = payload['URL']
                    req_method = payload['METHOD']
                    req_body = payload['BODY']
                    req_output = payload['OUTPUT']
                    row[col_detail] = payload['STATUS']
                    # print(req_body)
                    # return

                    if req_url != "" and req_method != "" and req_body != "":
                        try:
                            response = requests.request(req_method, req_url, headers=conf.headers,
                                                        data=json.dumps(payload['BODY']),
                                                        verify=False)
                            # response = requests.request(req_method, req_url, headers=headers, data=req_body, verify="CA_PATH")
                            row[col_result] = response.status_code
                            data = response.text

                            if req_output.strip() != "":
                                output_file = file_name.split(".")[0]
                                output_file = "{}/{}_{}_{}.csv".format(conf.output_folder, req_output, output_file,
                                                                       datetime.now().strftime("%Y_%m_%d"))
                                try:
                                    data = json.loads(data)
                                except:
                                    data = {}

                                if req_output == "outbound":
                                    if outbound_count == 0 and os.path.exists(output_file):
                                        os.remove(output_file)
                                    extract_outbound(data, msisdn, trxid, output_file)
                                    outbound_count += 1
                                elif req_output == "inbound":
                                    if inbound_count == 0 and os.path.exists(output_file):
                                        os.remove(output_file)
                                    extract_inbound(data, msisdn, trxid, output_file)
                                    inbound_count += 1
                                elif req_output == "nba":
                                    if nba_count == 0 and os.path.exists(output_file):
                                        os.remove(output_file)
                                    extract_nba(data, msisdn, trxid, output_file)
                                    nba_count += 1
                        except requests.exceptions.ConnectionError:
                            row[col_detail] = "Connection Error"
                    else:
                        row[col_detail] = "Missing request parameters"
                else:
                    row[col_detail] = payload['STATUS']

                print("Processing idx: {}, msisdn: {}, trxId: {}, response: {}".format(idx, msisdn, trxid,
                                                                                       row[col_detail]))
                resp_list.append(row)
                time.sleep(conf.delay_time/1000)

        with open(input_file, 'w', newline="\n") as f:
            write = csv.writer(f)
            write.writerow(header_col)
            write.writerows(resp_list)
