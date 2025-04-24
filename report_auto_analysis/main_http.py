# encoding: utf-8
import sys
import time
import json
import requests

crash_map = {
    "LNBSC1WK0RB001667": {"location": "上海市-上海城区-松江区-小昆山镇", "timestamp": 1712646592443, "warningId": "20240409150953000001", "vid": "LNBQPEYE6FGCWLMB6"},
    "LNBSC1WK0RB001216": {"location": "广东省-深圳市-光明区-新湖街道", "timestamp": 1712814361864, "warningId": "20240411134602000001", "vid": "LNBQREFNVVG7NJZX8"},
    "LNBSC1VK7RB006544": {"location": "辽宁省-沈阳市-浑南区-东湖街道", "timestamp": 1713527272045, "warningId": "20240419194752000002", "vid": "LNBQUCCBK7E0MFW50"},
    "LNBSC1WK5RB000952": {"location": "浙江省-宁波市-北仑区-霞浦街道", "timestamp": 1713704960668, "warningId": "20240421210921000003", "vid": "LNBQ5VBZZCYFHVM05"},
    "LNBSC1VK8RB003572": {"location": "北京市-北京城区-丰台区-南苑街道", "timestamp": 1713706696982, "warningId": "20240421213817000001", "vid": "LNBQ2KASTFRTUA8S3"},
    "LNBSC1WK5RB002331": {"location": "江苏省-常州市-溧阳市-天目湖镇", "timestamp": 1714207504415, "warningId": "20240427164518000001", "vid": "LNBQK7VR8X3F485S8"},
    "LNBSC1WK1RB000754": {"location": "辽宁省-大连市-甘井子区-兴华街道", "timestamp": 1714342048559, "warningId": "20240429060729000001", "vid": "LNBQYXFXFH1DBVH30"},
    "LNBSC1WK1RB001113": {"location": "广东省-惠州市-惠城区-陈江街道", "timestamp": 1714348048205, "warningId": "20240429074728000001", "vid": "LNBQ5ACCKM2AFDVK9"},
    "LNBSC1VK5RB009295": {"location": "湖北省-孝感市-汉川市-新堰镇", "timestamp": 1714726507350, "warningId": "20240503165508000003", "vid": "LNBQ8PJSRXET4TTV3"},
    "LNBSC1VK5RB005506": {"location": "山东省-青岛市-胶州市-胶莱街道", "timestamp": 1714738369592, "warningId": "20240503201250000002", "vid": "LNBQPFVB0FJBPBXC2"},
    "LNBSC1VK0RB004277": {"location": "上海市-上海城区-松江区-松江工业区", "timestamp": 1714880982546, "warningId": "20240505114943000001", "vid": "LNBQHVEZSHRZP3TC7"},
    "LNBSC1WK2RB003937": {"location": "广东省-深圳市-光明区-玉塘街道", "timestamp": 1715670238931, "warningId": "20240514150359000001", "vid": "LNBQ2P46MGF1UV2B1"},
    "LNBSC1VK5RB002864": {"location": "江苏省-镇江市-丹阳市-丹北镇", "timestamp": 1715730298866, "warningId": "20240515074459000003", "vid": "LNBQK6SCR5TACMYG8"},
    "LNBSC1VK5RB011063": {"location": "广东省-揭阳市-榕城区-榕东街道", "timestamp": 1716653171271, "warningId": "20240526000612000001", "vid": "LNBQUP29G6EDHUMR1"},
    "LNBSC1VK3RB004578": {"location": "山东省-聊城市-东昌府区-柳园街道", "timestamp": 1716769972611, "warningId": "20240527083253000001", "vid": "LNBQWAMFZHBFGVJB5"},
    "LNBSC1VK9RB012149": {"location": "浙江省-宁波市-慈溪市-白沙路街道", "timestamp": 1716888804165, "warningId": "20240528173324000002", "vid": "LNBQCDPVGPC3ALUK2"},
    "LNBSC1VK7RB011257": {"location": "四川省-成都市-双流区-兴隆镇", "timestamp": 1717808343159, "warningId": "20240608085904000001", "vid": "LNBQTK7A2R4ZDY0G0"},
    "LNBSC1WK0RB009851": {"location": "贵州省-遵义市-红花岗区-新蒲街道", "timestamp": 1717837831348, "warningId": "20240608171032000001", "vid": "LNBQPDVHFY58L2DH1"},
    "LNBSC1WK7RB000869": {"location": "青海省-海南藏族自治州-共和县-黑马河镇", "timestamp": 1718079097313, "warningId": "20240611121141000001", "vid": "LNBQR5KGKPCJWEVA5"},
    "LNBSC1VK5RB018188": {"location": "甘肃省-定西市-安定区-馋口镇", "timestamp": 1718542625040, "warningId": "20240616205706000001", "vid": "LNBQECYR8KGELVME7"},
    "LNBSC1VK2RB004636": {"location": "上海市-上海城区-宝山区-友谊路街道", "timestamp": 1718670915642, "warningId": "20240618083516000001", "vid": "LNBQ36SESSTWY2RH8"},
    "LNBSC1WK6RB012608": {"location": "湖北省-武汉市-汉南区-邓南街道", "timestamp": 1719320511397, "warningId": "20240625210152000001", "vid": "LNBQ3VYZ2EJREA754"},
    "LNBSC1WK7RB006929": {"location": "浙江省-金华市-兰溪市-赤溪街道", "timestamp": 1719328042524, "warningId": "20240625230723000001", "vid": "LNBQ5JHN2NZ8V9RL3"},
    "LNBSC1WKXRB021330": {"location": "四川省-成都市-金牛区-抚琴街道", "timestamp": 1719334555965, "warningId": "20240626005557000001", "vid": "LNBQ3CDMEBJUBK601"},
    "LNBSC1VK7RB015180": {"location": "重庆市-重庆城区-渝北区-大竹林街道", "timestamp": 1719473390871, "warningId": "20240627152951000001", "vid": "LNBQKK1CXUJ4RWH05"},
    "LNBSC1VK5RB015324": {"location": "重庆市-重庆城区-渝北区-大竹林街道", "timestamp": 1719565953643, "warningId": "20240628171234000001", "vid": "LNBQ5LFVLXZN3ATC5"},
    "LNBSC1VK9RB002916": {"location": "重庆市-重庆城区-江津区-鼎山街道", "timestamp": 1719595453600, "warningId": "20240629012414000001", "vid": "LNBQW3XUS5EFEBPZ2"},
    "LNBSC1VK9RB005248": {"location": "北京市-北京城区-海淀区-温泉镇", "timestamp": 1719656297779, "warningId": "20240629181819000001", "vid": "LNBQZZG9WULCSH0U2"},
    "LNBSC1VK5RB015128": {"location": "湖南省-长沙市-浏阳市-洞阳镇", "timestamp": 1719721568341, "warningId": "20240630122608000001", "vid": "LNBQF1GWRRBKTRS66"},
    "LNBSC1VK3RB021946": {"location": "四川省-眉山市-青神县-青竹街道", "timestamp": 1719728035499, "warningId": "20240630141356000001", "vid": "LNBQG4ETLDXTUJ4V0"},
    "LNBSC1VK7RB005670": {"location": "上海市-上海城区-青浦区-盈浦街道", "timestamp": 1719733665140, "warningId": "20240630154746000001", "vid": "LNBQWDFLCMPBHWSN2"},
    "LNBSC1VK9RB022048": {"location": "湖北省-武汉市-青山区-武钢实业公司", "timestamp": 1719958153325, "warningId": "20240703060914000001", "vid": "LNBQ5SJRXEGWW8LC4"},
    "LNBSC1VK5RB027005": {"location": "安徽省-阜阳市-颍上县-王岗镇", "timestamp": 1720065371158, "warningId": "20240704115611000001", "vid": "LNBQD2PMXGJRJJL70"},
    "LNBSC1WK2RB015182": {"location": "浙江省-嘉兴市-嘉善县-大云镇", "timestamp": 1720447723387, "warningId": "20240708220844000001", "vid": "LNBQT2H2ZHJPRNZC0"},
    "LNBSC1VK3RB033174": {"location": "江西省-九江市-柴桑区-永安乡", "timestamp": 1720567627325, "warningId": "20240710072707000001", "vid": "LNBQCYNEJRSBEHM83"},
    "LNBSC1VKXRB027761": {"location": "江西省-上饶市-信州区-沙溪镇", "timestamp": 1720577332662, "warningId": "20240710100853000001", "vid": "LNBQRFG7G7E7TMHE1"}
}


def run_workflow_zhangjian(vin, location, timestamp, warningId, vid, _workflow_id, _active_version,
                           _user="luhongxi",
                           _token="1a92127e1eda4a95be15a7f9d86fced3"):
    # URL
    url = f'https://api-gateway.dp.pt.xiaomi.com/openapi/develop/workflow/{_workflow_id}/run/all'

    # Headers
    headers = {
        'authorization': f'workspace-token/1.0 {_token}',
        'Content-Type': 'application/json'
    }

    # 入参准备
    if _workflow_id == "448944":
        dev_pre_pro = 0
    else:
        sys.exit()
    # Data
    _data = {
        "id": _workflow_id,
        "user": _user,
        "activeVersion": _active_version,
        "variables": [
            {"variableName": "${var:vin}", "variableValue": vin},
            {"variableName": "${var:location}", "variableValue": location},
            {"variableName": "${var:timestamp}", "variableValue": timestamp},
            {"variableName": "${var:warningId}", "variableValue": warningId},
            {"variableName": "${var:vid}", "variableValue": vid},
            {"variableName": "${var:dev_pre_pro}", "variableValue": dev_pre_pro}
        ]
    }

    # Send POST request
    _response = requests.post(url, headers=headers, data=json.dumps(_data))

    print(json.dumps(_data))

    # Return response
    return _response.text


if __name__ == '__main__':

    # 接口参数
    bUser = int(sys.argv[1])
    _active_version = int(sys.argv[2])
    _token = "8126cde8a10f4326a0157d2201277714"
    _workflow_id = "448944"
    if bUser == 0:  # nohup python main_http.py 0 1 > output.log 2>&1 &
        _workflow_id = "448944"
        # 初始化计数器
        count = 0
        for vin, details in crash_map.items():
            location = details['location']
            timestamp = details['timestamp']
            warningId = details['warningId']
            vid = details['vid']
            response = run_workflow_zhangjian(vin, location, timestamp, warningId, vid, _workflow_id, _active_version)
            # 每处理一个任务，计数器加1
            count += 1
            # 检查是否已处理2个任务
            if count % 2 == 0:
                print("Reached 2 tasks, pausing for 5 minutes...")
                time.sleep(300)  # 暂停600秒，即10分钟
    # else:
    #     if bUser == 1:
    #         vin = "LNBSC1WK1RB001113"
    #         date = 20240429
    #     elif bUser == 2:
    #         vin = "LNBSC1VK5RB005506"
    #         date = 20240503
    #     elif bUser == 3:
    #         vin = "LNBSC1VK5RB002864"
    #         date = 20240515
    #     elif bUser == 4:
    #         vin = "LNBSC1VK5RB011063"
    #         date = 20240526
    #     elif bUser == 5:
    #         vin = "LNBSC1VK3RB004578"
    #         date = 20240527
    #     else:
    #         sys.exit()
    #
    #     print(f"*** UI请求调度数据工场任务（workflowId={_workflow_id}）...")
    #
    #     # 发起工作流（后续任务在数据工场中流式作业）
    #     response = api.run_workflow(vin, date, _workflow_id, _active_version)
    #     print(response)
