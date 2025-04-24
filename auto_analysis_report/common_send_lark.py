import requests
import json


# 天璇机器人（量产环境）
client_id = "cli_a5f50026d7389062"
client_secret = "gtZzO3Z8jMf7z3S4HrzzkMUttBoIqjdM"


# 获取租户访问令牌
def get_tenant_access_token(client_id, client_secret):
    url = "https://open.f.mioffice.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "app_id": client_id,
        "app_secret": client_secret
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an error for non-200 status codes
        return response.json().get('tenant_access_token')
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None


# 发送消息
def send_lark_message_batch(user_id_list, event_title, contents):
    access_token = get_tenant_access_token(client_id, client_secret)
    if not access_token:
        print("Failed to retrieve tenant access token.")
        return

    # 创建消息内容
    elements = [{"tag": "div", "text": {"content": content, "tag": "lark_md"}} for content in contents]
    json_data = {
        "config": {"wide_screen_mode": True},
        "elements": elements,
        "header": {"template": "blue", "title": {"content": event_title, "tag": "plain_text"}}
    }

    message_batch_body = {
        "msg_type": "interactive",
        "card": json_data,
        "user_ids": user_id_list
    }

    headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json; charset=utf-8"}

    print("messageBatchBody信息", json.dumps(message_batch_body))
    print("headers信息", json.dumps(headers))

    try:
        response = requests.post("https://open.f.mioffice.cn/open-apis/message/v4/batch_send/", json=message_batch_body, headers=headers)
        response.raise_for_status()  # Raise an error for non-200 status codes
        send_common_rsp = response.json()
        print("sendCommonRsp信息", json.dumps(send_common_rsp))
        return send_common_rsp
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        send_common_rsp = {"code": -1, "msg": str(e)}
        return send_common_rsp


# user_id_list = ["yangpengfei", "luhongxi"]
# event_title = "[整车监控预警平台/模型预警]杨鹏飞测试"
# contents = ["车辆阶段：量产车", "车架号：测试","预警时间：2024-04-24 00:00:00"]
#
# send_common_rsp = send_lark_message_batch(user_id_list, event_title, contents)
# print(send_common_rsp)
