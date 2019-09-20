import requests
import pandas as pd


# 获取token
def get_token():
    result = requests.post('http://services.kurite.cn/huoban/getToken',
                           data={'id': '12067898212', 'pwd': 'xjoijsdif2112'})
    return result.json()
    # print(result.json())


# 根据当前token，获取对应表格的信息
def get_table(token):
    param = {'token': token, 'table': '5997216'}  # , 'name': ''
    try:
        result = requests.get('http://services.kurite.cn/huoban/getFields', params=param)
        if result.content:
            data = result.json()
        else:
            data = '暂未获取到数据'
    except Exception as e:
        print(e)
    # print(data)
    return data


# 获取表格内容
def get_table_info(current_token, column_id):
    # 获取对应字段的id
    # for i in column_id:
    #     print(i['name'])
    #     # print(i['name'])
    #     if i['name'] == "经销商代码":
    #         dealer_id = i['field']
    #     if i['name'] == "经销商名称":
    #         dealer_name = i['field']
    #     if i['name'] == "文件名过滤关键字":
    #         file_filter = i['field']
    #     if i['name'] == "subsidiaryMark":
    #         sub = i['field']

    # 请求体必要信息
    datas = {
        'token': current_token,
        'table': '5997216'
        # , 'search': {
        #     'fields': [dealer_name]
        #     , 'keywords': ['目标名称']
        # }
        # , 'where': {
        #     'and': [
        #         {
                    # 'field': dealer_id,
                    # 'query': {
                    #     'eq': 'B_00240Q'
                    # }
        #         }
        #     ]
        # }
        # ,'offset':5
    }
    # 调用post请求，获取response
    detail_info = requests.post('http://services.kurite.cn/huoban/getTableInfo', data=datas)
    return detail_info


if __name__ == "__main__":
    res_json = get_token()
    # 获取response中的token
    current_token = res_json['data']['token']
    print(current_token)
    # 获取当前表格信息
    field_info = get_table(current_token)
    column_id = field_info['data']
    # 获取表格内容
    table_info = get_table_info(current_token, column_id)
    tab_json = table_info.json()
    zkb_data = tab_json['data']['tableInfo']
    df = pd.DataFrame(zkb_data)
    print(df)
    zkb_path = "C:/Users/qttu01/Documents/总控表/zkb.csv"
    df.to_csv(zkb_path, encoding="utf_8_sig")
