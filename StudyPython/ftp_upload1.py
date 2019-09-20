import ftplib
import pymongo
import sys
import os
import os.path
import logging
import glob
import hashlib
import time
import re
import pandas as pd
from logging.handlers import TimedRotatingFileHandler
import configparser
# 引入Counter将不同名同内容文件拿到
from collections import Counter
# 移动文件到另一个文件夹
import shutil
import datetime
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

ftp = ftplib.FTP()
# 创建保存第一次上传文件的csv表路径
file_hash_dir = "./firsthash.csv"
# 扩展名
# extensions = ['csv', 'xlsx', 'xlsxs']
log_name_list = []  # 保存log文件名的集合
# 定义要上传的文件的文件夹路径
# local_dir_name = r'C:\Users\qttu01\Documents\b'
# # 为已上传的文件改名添加的前缀
# insert_prefix_name = "down_"
# # 定义文件名匹配名
# matching_name = "SM"
# host = "106.14.198.167"
# username = "admin"
# password = "Tt469xxx"
# # jxs提供的FTP已有的目录信息
# ftpdir = r'a/b/c'
# # log日志存放路径及文件hash码存放位置
# log_save_dir = r'C:\Users\qttu01\Documents\b'
# # hash码文件存放位置 与log文件存放位置可一致
# hash_save_dir = r'C:\Users\qttu01\Documents\b\filehash.csv'

# 导入配置文件
cf = configparser.ConfigParser()
# 配置文件含有中文时使用utf-8-sig
# cf.read('config.ini', encoding='utf-8-sig')
# host = cf.get('FTP', 'host')
# username = cf.get('FTP', 'username')
# password = cf.get('FTP', 'password')
# # 定义要上传的文件的文件夹路径
# local_dir_name = cf.get('File', 'local_dir_name')
# # 定义文件名匹配名
# matching_name = cf.get('File', 'matching_name')
# # jxs提供的FTP已有的目录信息
# ftpdir = cf.get('File', 'ftp_dir')
# # 为已上传的文件改名添加的前缀
# insert_prefix_name = cf.get('File', 'insert_prefix_name')
# # log日志存放路径
# log_save_dir = cf.get('File', 'log_save_dir')
# # hash码文件存放位置
# hash_save_dir = cf.get('File', 'hash_save_dir')
#
#
# 打印日志
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
# 添加TimedRotatingFileHandler 按照时间回滚、分割日志
# 定义一个1秒换一次log文件的handler
# 保留3个旧log文件
timefilehandler = TimedRotatingFileHandler("./log.log", when='d', interval=1,  # 隔一天更新一次log日志
                                           backupCount=10)  # 第一个参数filename是输出日志文件名的前缀
timefilehandler.setLevel(logging.DEBUG)
# 设置后缀名称，跟strftime的格式一样
timefilehandler.suffix = "%Y-%m-%d_%H-%M-%S.log"
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # 当前时间-Logger的名字-文本形式的日志级别-用户输出的消息
timefilehandler.setFormatter(formatter)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)
logger.addHandler(timefilehandler)
# 将日志打印到控制台
logger.addHandler(console)


def connectFtp(host_args, port=21):  # 连接ftp
    try:
        ftp.connect(host_args, port)
        logger.info("连接ftp网址中......")
        print("连接ftp网址中......")
    except Exception:
        print("err_ftp网址有误")


def login(username_args, password_args):  # 登陆ftp
    try:
        ftp.login(username_args, password_args)
        logger.info("登陆成功")
        print("登陆成功")
    except Exception:
        logger.error("用户名或密码错误")
        print("err_用户名或密码错误")


def matching_filename(local_dir_name_args, matching_name_args):  # 得到符合文件匹配规则的文件
    files_list = []  # 符合特征的所有文件
    for matching_name in matching_name_args:
        files_list.extend(glob.glob(local_dir_name_args + '\\*' + matching_name + '*.csv'))

    return files_list


def matching_file_upload(local_dir_name_args, insert_prefix_name_args,
                         matching_name_args):  # 得到已上传过的文件（改名后的）
    file_upload = []  # 已改名上传后的文件
    # 文件指定的后缀名避免上传其他格式的文件
    for matching_name in matching_name_args:
        file_upload.extend(glob.glob(
            local_dir_name_args + '\\' + insert_prefix_name_args + '*' + matching_name + '*.csv'))
        file_upload.extend(glob.glob(
            local_dir_name_args + '\\' + insert_prefix_name_args + '*' + matching_name + '*.xlsx'))
    return file_upload


def matching_file_not_upload(local_dir_name, matching_name, insert_prefix_name):  # 得到未上传的文件
    # 使用set集合获得集合的差拿到未上传文件名
    set1 = set(matching_filename(local_dir_name, matching_name))
    set2 = set(matching_file_upload(local_dir_name, insert_prefix_name, matching_name))
    file_not_upload = set1 - set2
    file_not_upload = list(file_not_upload)
    print(file_not_upload)
    f = []
    for file in file_not_upload:
        size = os.path.getsize(file)
        print(size)
        if size <= 6633:
            f.append(file)
            print("此文件内容为空" + file)
            logger.warning("此文件内容为空" + file)
    for f1 in f:
        send_email("上传的文件内容为空", file + "此文件内容为空，上传失败，请及时查看，谢谢!")
        file_not_upload.remove(f1)
    print(file_not_upload)
    return file_not_upload


def matching_file_hash(hash_save_dir_args, upload_file_hash, upload_file_args, ftpdir, local_dir_name,
                       insert_prefix_name):  # 匹配文件的hash值判断是否已上传过了
    # 过滤
    writer_excel_file_hash_first(upload_file_args, upload_file_hash)
    # 拿到hash码存放的文件
    df = pd.read_csv(file_hash_dir, encoding='utf-8')
    hashlist = df['文件hash码'].tolist()
    hashname = df['文件名'].tolist()
    # 定义一个过滤完后文件内容重复的集合
    filter_upload = []
    # 拿出不同名同内容和文件hash码
    con = dict(Counter(upload_file_hash))
    repeat = [key for key, value in con.items() if value > 1]  # 只展示重复元素
    print("重复的hash", repeat)
    if repeat:
        i = 0
        for h in hashlist:
            for r in repeat:
                if r == h:
                    print("同名文件——————", hashname[i])
                    filter_upload.append(hashname[i])
                    print(hashname[i], "出现了不同名同内容文件")
                    logger.warning("出现了同名文件")
                    logger.warning(hashname[i])
            i += 1
    # 将重复文件移动到另一个文件夹
    for f in filter_upload:
        send_email("文件内容已被上传过", f+"此文件的文件内容已被上传过")
        print(f, "-------------------------------重复文件")
        re_filename = local_dir_name + "\\repeat" + os.path.basename(f)
        try:
            os.renames(f, re_filename)
        except Exception:
            print(f, "改名后文件已存在，修改失败")
            logger.error(f, "改名后文件已存在，修改失败")
        # 定义保存不同名同内容的文件夹
        repeat1 = local_dir_name + "\\repeat"
        # 定义包含repeat文件的集合
        repeatlist = []
        if os.path.exists(repeat1):
            repeatlist.extend(glob.glob(local_dir_name + '\\*repeat*.csv'))
            print("重复文件集合-------------------------------------")
            for r in repeatlist:
                shutil.move(r, repeat1)
        else:
            os.makedirs(repeat1)
    # 文件名
    set2 = set(filter_upload)
    set1 = set(upload_file_args)
    set3 = set1 - set2
    list_1 = list(set3)
    # hash码
    set_1 = set(upload_file_hash)
    set_2 = set(repeat)
    set_3 = set_1 - set_2
    list_2 = list(set_3)
    # 上传
    if os.path.exists(hash_save_dir_args):
        print(hash_save_dir_args)
        # 拿到hash码存放的文件
        df = pd.read_csv(hash_save_dir_args, encoding='utf-8')
        print(df)
        hashlist1 = df['文件hash码'].tolist()
        hashname1 = df['文件名'].tolist()
        # 定义一个过滤完后已被上传过的文件的集合
        filter_upload = []
        # 定义一个过滤后的hash码集合
        filter_upload_hash = []
        i = 0
        for li in list_2:
            j = 0
            for lis in hashlist1:
                if i > j:
                    break
                if li == lis:
                    filter_upload_hash.append(lis)
                    filter_upload.append(hashname1[j])
                    # print(hashname1[j] + "此文件已上传过")
                    logger.warning("已上传过的文件中与" + hashname1[j] + "为同一内容文件，且已上传过")
                j += 1
            i += 1
        # 过滤完后调用上传方法 注意:若有已上传过的文件上传参数会发生改变 在此处调用上传的方法即可
        # 文件名
        set2 = set(filter_upload)
        set1 = set(list_1)
        set3 = set1 - set2
        list1 = list(set3)
        # hash码
        print(upload_file_hash)
        set_1 = set(list_2)
        set_2 = set(filter_upload_hash)
        set_3 = set_1 - set_2
        list2 = list(set_3)
        if list1:
            # 调用上传方法
            try:
                writer_excel_file_hash(hash_save_dir_args, list1, list2)  # 添加到filehash文件中
            except Exception:
                logger.error("上传失败")
            else:
                print("有新文件需要上传")
                logger.info("有新文件需要上传")
                # for li in list1:
                #     uploadFile(li, ftpdir, local_dir_name, insert_prefix_name)

        else:
            logger.info("无新文件需要上传")
    else:
        # 第一次上传时需调用上传文件的方法
        logger.info("filehash.csv文件还未创建或已被删除，可继续上传")
        print("filehash.csv文件还未创建或已被删除，可继续上传")
        writer_excel_file_hash(hash_save_dir_args, list_1, list_2)
        # for file in list_1:
        #     uploadFile(file, ftpdir, local_dir_name, insert_prefix_name)


# sha1加密
def read_file_hash(filename):  # 通过文件名获取文件的hash编码
    # 创建一个哈希对象
    h = hashlib.sha1()
    # 以二进制读取模式打开一个文件
    with open(filename, 'rb') as f:
        # 循环直到文件结束
        chunk = 0
        while chunk != b'':
            # 一次只读取1024个字节
            chunk = f.read(1024)
            h.update(chunk)
    return h.hexdigest()


# _FILE_SLIM = (100 * 1024 * 1024)


# MD5加密
# def file_md5(filename):
#     calltimes = 0
#     hmd5 = hashlib.md5()
#     fp = open(filename, 'rb')
#     f_size = os.stat(filename).st_size
#     if f_size > _FILE_SLIM:
#         while (f_size > _FILE_SLIM):
#             hmd5.update(fp.read(_FILE_SLIM))
#             f_size /= _FILE_SLIM
#             calltimes += 1
#         if (f_size > 0) and (f_size < _FILE_SLIM):
#             hmd5.update(fp.read())
#     hmd5.update(fp.read())
#
#     return hmd5.hexdigest()


def get_not_upload_hash(file_upload_args):  # 获取未上传文件的hash值 集合需在方法体内定义，否则会出现值重复现象
    list1 = []
    for file in file_upload_args:
        list1.append(read_file_hash(file))
    return list1


def uploadFile(filename, ftpdir, local_dir_name, insert_prefix_name):  # 上传文件
    print("开始上传")
    file_name = os.path.basename(filename)
    file_open = open(filename, 'rb')
    servername = ftpdir + "/" + file_name
    try:
        ftp.storbinary('STOR ' + servername, file_open, 4096)
    except Exception as result:
        print("上传失败...............ftp未打开" + format(result))
        send_email("文件上传失败", filename + "文件上传失败，可能是ftp未打开")
    else:
        print(filename + "上传成功")
        logger.info(filename + "上传成功")
        # 需关闭后才能进行重命名
        file_open.close()

        # 上传后的文件重命名
        # 添加前缀
        re_filename = local_dir_name + "\\" + insert_prefix_name + file_name
        try:
            os.renames(filename, re_filename)
        except Exception:
            print(filename, "文件已存在，修改失败")
            logger.error(filename, "文件已存在，修改失败")


def print_hash_log():  # 将上传后的文件hash值打印到logging中
    for file_hash in get_not_upload_hash():
        for file in matching_file_not_upload():
            logger.info("上传文件的hash值与文件名：" + file + file_hash)


def writer_excel_file_hash(hash_save_dir_args, file_upload_args, hash_list_args):  # 将文件名与文件hash码打印到csv表中
    today = datetime.date.today()
    # time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))年月日时分秒
    dit = {'time': today, '文件名': file_upload_args, '文件hash码': hash_list_args}
    try:
        df = pd.DataFrame(dit)
    except Exception as e:
        logger.error("文件数与hash码获取错误")
        print("文件数与hash码获取错误")
        return e
    else:
        if os.path.exists(hash_save_dir_args):
            # to_csv默认模式时'w',改成'a'便可以实现列的追加 header默认参数时true，改成False可以避免表头也追击
            df.to_csv(hash_save_dir_args, mode='a', index=False, sep=',', header=False)
        else:
            # to_csv乱码时使用'utf_8_sig', encoding='utf_8_sig'改了之后会影响scv的打开
            df.to_csv(hash_save_dir_args, columns=['time', '文件名', '文件hash码'], index=False, sep=',')


def writer_excel_file_hash_first(file_upload_args, hash_list_args):  # 将文件名与文件hash码打印到csv表中
    dit = {'文件名': file_upload_args, '文件hash码': hash_list_args}
    try:
        df = pd.DataFrame(dit)
    except Exception as e:
        logger.error("fist文件数与hash码获取错误")
        print("first文件数与hash码获取错误")
        return e
    else:
        df.to_csv(file_hash_dir, columns=['文件名', '文件hash码'], index=False, sep=',')


def to_csv_datetime(file_hash_dir_args, hash_dir_file_args):
    if os.path.exists(hash_dir_file_args) and os.path.getsize(hash_dir_file_args) > 6633:
        filehash = []
        filehash.extend(glob.glob(file_hash_dir_args + '\\*filehash*.csv'))
        t1 = datetime.datetime.today().date()
        tnum = {}
        # 遍历文件名日期
        for f in filehash:
            basename = os.path.basename(f)
            b = basename[8:18]  # 拿到日期
            t2 = datetime.datetime.strptime(b, '%Y-%m-%d').date()
            t3 = t1 - t2
            tnum[f] = t3.days
        tmin = min(tnum.values())
        # 拿到离当前日期最近的文件
        filehash1 = [k for k, v in tnum.items() if v == tmin][0]
        df = pd.read_csv(filehash1, encoding="utf-8")
        df1 = np.array(df)
        filehash3 = []
        try:
            # 拿到文件内容中规定时间段的日期
            filetime = df['time'].tolist()
            for i in range(len(filetime)):
                timedate = datetime.datetime.strptime(filetime[i], '%Y-%m-%d').date()
                days = t1 - timedate
                if days.days < 180:
                    filehash3.append(df1[i].tolist())
        except Exception:
            print("有hash文件没有上传hash数据而发生的异常,无关紧要")
            logger.warning("有hash文件没有上传hash数据而发生的异常,无关紧要")
        new = pd.DataFrame(filehash3)
        t1 = str(t1)
        new_dir = file_hash_dir_args + "\\filehash" + t1 + ".csv"
        new.to_csv(new_dir, index=False, sep=',')
        table = pd.read_csv(new_dir, encoding='utf-8')  # pd.read_csv(file_hash_dir, encoding='utf-8')
        df = pd.DataFrame(table)
        df.rename(columns={'0': 'time', '1': '文件名', '2': '文件hash码'}, inplace=True)
        df.to_csv(new_dir, index=False, sep=',')


def send_email(title, content):  # 发送邮件
    try:
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = formataddr([sender, my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr([receiver, my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = title  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是465
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        print("邮件发送失败")
        logger.error("邮件发送失败")
    else:
        print("邮件发送成功")
        logger.info("邮件发送成功")


# def uploadcode(localdir, filename):
#     df = pd.read_excel(filename)
#     basename = os.path.basename(filename)
#     if "A" in basename:
#         print("A")
#         print(df['CSL WS Code'])
#     if "B" in basename:
#         print(df['WS Name'])
#         print("B")
#     df = pd.read_excel(filename)
#     if df['WS Name'] or df['CSL WS Code']:
#         print(df['WS Name'].drop_duplicates())
#         print(df['CSL WS Code'].drop_duplicates())


def close():
    ftp.close()


# 配置文件
df = pd.read_excel('./ftp_detail.xlsx')
df = pd.DataFrame(df)
e = df['id']
a = df['ftpServerAddress']
b = df['ftpuser']
c = df['ftppassword'].tolist()
d = df['ftpfloder'].tolist()
f = df['matching'].tolist()
g = df['prefix'].tolist()
h = df['localFloder'].tolist()
j = df['hashFloder'].tolist()

# 邮箱账号设置
my_sender = '2338581366@qq.com'  # 发件人邮箱账号
my_pass = 'kipugvhehxonebbb'  # 发件人邮箱密码(当时申请smtp给的口令)
my_user = '2806558702@qq.com'  # 收件人邮箱账号，我这边发送给自己
sender = "曹丹"
receiver = "发送的对象"

for i in range(len(a)):
    print("连接..............", e[i])
    connectFtp(a[i])
    login(b[i], c[i])
    id = str(e[i])
    hash_dir = j[i] + "\\hash" + id
    print(id)
    today = str(datetime.datetime.today().date())
    if os.path.exists(hash_dir):
        print("hash文件夹已存在")
        hash_dir_file = hash_dir + "\\filehash" + today + ".csv"
    else:
        print("hash文件夹不存在，需创建")
        os.makedirs(hash_dir)
        print("hash文件夹已创建")
        hash_dir_file = hash_dir + "\\filehash" + today + ".csv"
    to_csv_datetime(hash_dir, hash_dir_file)
    not_upload = matching_file_not_upload(h[i], f[i].split(','), g[i])
    not_upload_hash = get_not_upload_hash(not_upload)
    matching_file_hash(hash_dir_file, not_upload_hash, not_upload, d[i], h[i], g[i])
    close()
# 先判断hash文件中是否已上传过此文件
# matching_file_hash(hash_save_dir_args, upload_file_hash, upload_file_args, matching_file_not_upload_args, ftpdir, local_dir_name, insert_prefix_name)

# 上传文件
# 若未上传过此外文件先上传再将上传过的文件名及文件添加到hash文件中
# 方法若返回的是集合，在下一个方法调用此集合时应作为参数传入
