import pymongo
import datetime
from dateutil import parser
import pandas as pd
import time
import os
import glob


def create_flag():
    if os.path.exists(flag_dir):
        print("flag文件夹已创建")
    else:
        os.makedirs(flag_dir)
        print("flag文件夹创建成功")
    flag_name = flag_dir + "/flag_" + datetime.datetime.now().strftime("%Y%m%d%H%M") + ".csv"
    flag_list = []
    flag_list.extend(glob.glob(flag_dir + "/flag_*.csv"))
    if flag_list:
        print("已有flag文件存在")
        file = flag_list[0]
        basename = os.path.basename(file)
        file_time = basename[5:16]
        # 得到两小时前的时间
        beforetime = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H%M')
        print(file_time)
        print(beforetime)
        if file_time < beforetime:
            print("已超过两小时可运行")
            os.remove(file)
            df = pd.DataFrame()
            df.to_csv(flag_name)
            return True
        else:
            print("这是两小时之内的时间，还不能重新运行")
            return False
    else:
        df = pd.DataFrame()
        df.to_csv(flag_name)
        return True


def export_mongodb():
    client = pymongo.MongoClient(host='dds-uf6496d8fab38834-pub.mongodb.rds.aliyuncs.com', port=3717)
    db = client['mostar']  # 指定数据库名
    db.authenticate('aditestro', 'Tt469789xx')  # 获取权限
    # gk销售表
    gk_xiao = db['gk_xiao']
    # 贝林销售表
    beilin_xiao = db['beilin_xiao']
    # clientlogs表
    clientlogs = db['clientlogs']
    # ISO日期格式
    today_str = nowdate.strftime("%Y-%m-%dT00:00:00.000Z")
    # 打开当月打单表
    fright_df = pd.read_csv(fright_zkb_path, encoding="utf_8_sig")
    today_ISO = parser.parse(today_str)  # 使用parser包转换成能识别ISO的日期类型
    df = pd.read_csv(contrast_name_path, encoding='utf_8_sig')
    # 拿到今日上传的数据logs的customerid与email
    clientlogs_datas = clientlogs.find({"createdAt": {'$gte': today_ISO}, "request": "upload"})
    customer_id_alllist = []  # 保存clientslogs中customersid值的
    # 拿到对应的customerid
    for log in clientlogs_datas:
        customer_id_alllist.append(log['customer'])
    # 当对照表中已存在数据时，拿出已导出过且不用再次更新的id
    # 再用所有的id-已导出过不用更新的id = 此次需要导出的id
    customer_id_list = sorted(set(customer_id_alllist), key=customer_id_alllist.index)  # 去除重复id
    # print(customer_id_list, "去重复后的clientslogs---customersid")
    # 将对照表中的customerid拿出来，
    customer_id_contrast = list(df['customersid'])  # 表格里的的也需要去重复
    customer_id_contrast1 = sorted(set(customer_id_contrast), key=customer_id_contrast.index)
    # print(customer_id_contrast1, "去重复后对照表中的customersid")
    customersid_same = []
    for customersid in customer_id_list:
        # 测试是否已导出过
        for i in customer_id_contrast1:
            if len(str(i)) > 3:
                if str(customersid) == str(i):
                    # print(str(i), "相同的customersid")
                    customersid_same.append(customersid)
    # customersid_same1 = sorted(set(customersid_same), key=customersid_same.index)
    for customersid in customersid_same:
        customer_id_list.remove(customersid)
    # 拿到客户表
    customers = db['customers']
    if customer_id_list:
        for i in range(len(customer_id_list)):
            customer = customers.find({"_id": customer_id_list[i]})
            # 将email与customerid放于字典中
            for c in customer:
                df.loc[df['email'] == c['email'], 'customersid'] = c['_id']
                df.to_csv(contrast_name_path, encoding="utf_8_sig", index=False)
    # subsidiaryMark为空的拿出来 这些可以不用去关联销售表匹配subsidiaryMark了
    # 但是需要查看email对应的经销商编码是否会有多个 在对照表中对应的经销商id相通即可
    # 将对照表中subsidiaryMark为空的拿出来
    df_sub_null = df[df['subsidiaryMark'].isna()]
    email_cusotmer = df_sub_null[df_sub_null['customersid'].notna()]  # customersid不为空
    email_cusotmer_list = email_cusotmer['customersid'].tolist()  # customersid
    deaclercode_list = email_cusotmer['dealercode'].tolist()  # 经销商代码，用来导入指定名字的

    # 当sub为空时直接导入数据
    find_xiao(email_cusotmer_list, gk_xiao, deaclercode_list, df, contrast_name_path)
    # find_xiao(email_cusotmer_list, beilin_xiao, deaclercode_list, df)

    df_sub_notnull = df[df['subsidiaryMark'].notna()]  # 拿到sub不为空的df
    email_cusotmer_1 = df_sub_notnull[df_sub_notnull['customersid'].notna()]  # customersid不为空 df
    sub_notnull_list = email_cusotmer_1['subsidiaryMark'].tolist()  # 拿到对照表中的sub list
    email_cusotmer_list_1 = email_cusotmer_1['customersid'].tolist()  # customersid的list
    dcodesubnonull_list = email_cusotmer_1['dealercode'].tolist()  # 经销商代码 list

    # 当sub不为空时直接导入数据
    find_sub_xiao(email_cusotmer_list_1, gk_xiao, sub_notnull_list, dcodesubnonull_list, df, contrast_name_path)
    # find_sub_xiao(email_cusotmer_list_1, gk_xiao, sub_notnull_list, dcodesubnonull_list, df)

    print("已完成运行")


def find_xiao(email_cusotmer_list, xiao_table, deaclercode_list, df, file_name1):
    for i in range(len(email_cusotmer_list)):
        datas = xiao_table.find(
            {"controlDate": {"$gte": first_day, "$lte": today}, "customer": email_cusotmer_list[i]})
        df1 = pd.DataFrame(list(datas))
        if df1.empty:
            print(deaclercode_list[i], "该经销商暂无新的销售数据")
        else:
            file_name = "ADI_" + deaclercode_list[i] + "_" + today + ".xlsx"
            df1.to_excel(
                input_path + "/" + deaclercode_list[i] + "/" + file_name,
                encoding="utf_8_sig")
            df.loc[df.dealercode == deaclercode_list[i], 'state'] = "已导出"
            df.loc[df.dealercode == deaclercode_list[i], 'filename'] = file_name
            # df.loc[df.dealercode == deaclercode_list[i], 'date'] = today_name
            df.to_csv(file_name1, encoding="utf_8_sig", index=False)
            print(deaclercode_list[i], "此经销商已完成导出")


def find_sub_xiao(email_cusotmer_list_1, xiao_table, sub_notnull_list, dcodesubnonull_list, df, file_name1):
    # 当sub不为空时需做判断
    for i in range(len(email_cusotmer_list_1)):
        datas = xiao_table.find(
            {"controlDate": {"$gte": first_day, "$lte": today}, "customer": email_cusotmer_list_1[i],
             "subsidiaryMark": sub_notnull_list[i]})
        df1 = pd.DataFrame(list(datas))
        if df1.empty:
            print(dcodesubnonull_list[i], "该经销商暂无新的销售数据")
        else:
            file_name = "ADI_" + dcodesubnonull_list[i] + "_" + today + ".xlsx"
            df1.to_excel(
                input_path + "/" + dcodesubnonull_list[i] + "/" + file_name,
                encoding="utf_8_sig")
            df.loc[df.dealercode == dcodesubnonull_list[i], 'state'] = "已导出"
            df.loc[df.dealercode == dcodesubnonull_list[i], 'filename'] = file_name
            # df.loc[df.dealercode == dcodesubnonull_list[i], 'date'] = today_name
            df.to_csv(file_name1, encoding="utf_8_sig", index=False)
            print(dcodesubnonull_list[i], "此经销商已完成导出")


def create_input():
    # 打开zkb表
    df = pd.read_excel(zkb_path + zkb_name)
    df_dir = df[df['状态'] == 1]  # 直连的经销商
    df_dir1 = df_dir[df_dir['备注'].notna()]
    # 实时更新总控表打单名单数据 由于是追加a模式，故删除后再创建即可
    if os.path.exists(fright_zkb_path):
        os.remove(fright_zkb_path)
    for i in range(len(fright_same)):
        # 模糊查询当月打单名单 df_dir1.loc[df_dir1['备注'].str.contains("当月打单名单")]
        df_this_month = df_dir1[df_dir1['备注'] == fright_same[i - 1]]
        # 将当月打单名单表表存到总控表同级目录下
        df_this_month.to_csv(fright_zkb_path, mode="a", index=False, sep=',', header=False, encoding="utf_8_sig")
    fright_df = pd.read_csv(fright_zkb_path, encoding="utf_8_sig")
    if os.path.exists(input_path):
        print("input文件夹已存在，无需再次创建")
    else:
        os.makedirs(input_path)
        print("input文件夹创建成功")
    if os.path.exists(contrast_path):
        print("contrast文件夹已存在，无需再次创建")
    else:
        os.makedirs(contrast_path)
        print("contrast文件夹创建成功")
    # 创建打单名单文件夹
    fright_code = fright_df[fright_df.columns[1]].tolist()
    for i in range(len(fright_code)):
        if os.path.exists(input_path + "/" + fright_code[i]):
            print(fright_code[i], "此文件夹已创建")
        else:
            os.makedirs(input_path + "/" + fright_code[i])
            # 创建history文件夹
            os.makedirs(input_path + "/" + fright_code[i] + "/" + "history")
            print(fright_code[i], "此文件夹创建成功")
            print(fright_code[i] + "/history", "此文件夹创建成功")
    contrast_file = []
    contrast_file.extend(glob.glob(contrast_path + "/contrast_*.csv"))
    if contrast_file:
        print("对照表已创建过")
    else:
        # 创建对照表的方法
        create_contrast(fright_df)


def create_contrast(fright_df):
    # 创建对照表  1 经销商代码  6 文件名过滤关键字   23 sub
    # contrast_df = fright_df[fright_df.columns[1], fright_df.columns[6], fright_df.columns[23]]
    contrast_dit = {"date": today_name, "dealercode": fright_df[fright_df.columns[1]],
                    "email": fright_df[fright_df.columns[6]],
                    "subsidiaryMark": fright_df[fright_df.columns[23]], "customersid": "", "clientlogsdate": "",
                    "filename": "", "state": ""}
    contrast_df = pd.DataFrame(contrast_dit)
    contrast_df.to_csv(contrast_name_path,
                       columns=["date", "dealercode", "email", "subsidiaryMark", "customersid", "clientlogsdate",
                                "filename", "state"]
                       , index=False, encoding='utf_8_sig')


zkb_path = "C:/Users/qttu01/Documents/总控表/"
zkb_name = "zkb.xlsx"
fright_zkb_path = zkb_path + "zkb_fright.csv"
# 放入配置文件 fright 打单
fright_same = ['贝林当月打单名单', 'LEO当月打单名单', '营销当月打单名单']
# 与input同级的目录 用来保存文件导出记录的
input_path_parent = "C:/Users/qttu01/Documents/"
# 定义flag目录，防止多次触发
flag_dir = input_path_parent + "flag"
# 创建input文件
input_name = "input"
input_path = input_path_parent + input_name
# 创建对照表的文件夹名 contract对照
contrast_name = "contrast"
contrast_path = input_path_parent + contrast_name
# 获取当前时间
nowdate = datetime.datetime.now()
today = nowdate.strftime("%Y%m%d")
# 保存到contrast中的时间戳
today_name = nowdate.strftime("%Y%m%d%H%M%S")
# contrast命名方式
contrast_name_path = contrast_path + "/contrast_" + today + ".csv"
# 获取上月第一天的数据
first_day = datetime.datetime(nowdate.year, nowdate.month - 1, 1).strftime('%Y%m%d')
# 温馨提示：方法参数最大长度6个
# if create_flag():
#     create_input()
#     export_mongodb()
# else:
#     print("停止运行")
create_input()
export_mongodb()