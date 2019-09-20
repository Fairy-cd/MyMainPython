from selenium import webdriver
# 下载PIL报错处理：打开cmd窗口输入pip install Pillow (python3版本)
from PIL import ImageGrab, Image
import pytesseract
import time
import datetime
from yundama import YDMHttp


def login():
    driver.get(host)
    # 输入账号密码
    driver.find_element_by_id("loginId").send_keys(user)
    driver.find_element_by_id("password").send_keys(password)


def verification_code():  # 识别验证码,返回验证码
    ImageGrab.grab((x, y, x1, y1)).save(imgpath)  # 截图保存验证码
    imageobject = Image.open(imgpath)  # 打开验证码图片
    # 图片进行置灰处理
    imageobject = imageobject.convert('L')
    # imageObject.show()
    # 二值化阈值
    threshold = 150
    table = []
    for i in range(256):
        if i < threshold:
            table.append(0)
        else:
            table.append(1)
    # 通过表格转换成二进制的图片，1的作用是白色，0是深色
    imageobject = imageobject.point(table, "1")
    # imageobject.show()
    print(pytesseract.image_to_string(imageobject))
    return pytesseract.image_to_string(imageobject)


def begin_login():
    if verification_code() == "":
        verification_code()
        begin_login()
    else:
        driver.find_element_by_id("imagecode").clear()
        driver.find_element_by_id("imagecode").send_keys(verification_code())
        driver.find_element_by_xpath('//*[@id="userDiv"]/p[5]/a').click()
        try:  # //*[@id="treeLeft"]/li/div/span[3] //*[@id='treeLeft']/li/ul/li[6]/div/span[4]
            driver.find_element_by_xpath("//*[@id='treeLeft']/li/ul/li[6]/div/span[4]").click()
        except Exception:
            verification_code()
            begin_login()
        else:
            print("登陆成功")


def get_flow():
    # 拿到当前时间
    today = datetime.datetime.now()
    # 获取上月第一天日期时间
    first_day = datetime.datetime(today.year, today.month - 1, 1).strftime("%Y-%m-%d")
    # 获取当月第一天
    now_first_day = datetime.datetime(today.year, today.month, 1)
    # 获取上月最后一天日期时间
    last = now_first_day - datetime.timedelta(days=1)
    last_day = last.strftime("%Y-%m-%d")
    driver.find_element_by_xpath("//*[@id='searchArea']/tbody/tr/td[1]/span[1]/input[1]").clear()
    driver.find_element_by_xpath("//*[@id='searchArea']/tbody/tr/td[1]/span[1]/input[1]").send_keys(first_day)
    driver.find_element_by_xpath('//*[@id="searchArea"]/tbody/tr/td[1]/span[2]/input[1]').clear()
    driver.find_element_by_xpath('//*[@id="searchArea"]/tbody/tr/td[1]/span[2]/input[1]').send_keys(last_day)
    driver.find_element_by_xpath('/html/body/div[1]/div/div[1]/table/tbody/tr/td[1]/a/span/span').click()


driver = webdriver.Chrome(r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe')
# 最大化窗口
driver.maximize_window()
host = 'http://221.133.237.227:801/EDI/Login'
user = '602272'
password = 'fjgd1234'
imgpath = r"C:\Users\qttu01\Desktop\图片\a.png"
x = 880
y = 480
x1 = 929
y1 = 500
ydmUsername = 'seaxw'  # 用户名
ydmPassword = 't46900_'  # 密码
appid = 1  # 开发者相关 功能使用和用户无关
appkey = '22cc5376925e9387a23cf797cb9ba745'  # 开发者相关 功能使用和用户无关
# 验证码类型，# 例：1004表示4位字母数字，不同类型收费不同。请准确填写，否则影响识别率。在此查询所有类型 http://www.yundama.com/price.html
codetype = 1004

login()
begin_login()
# get_flow()
# 拿到当前时间
# today = datetime.datetime.now()
# # 获取上月第一天日期时间
# first_day = datetime.datetime(today.year, today.month - 1, 1).strftime("%Y-%m-%d")
# # 获取当月第一天
# now_first_day = datetime.datetime(today.year, today.month, 1)
# # 获取上月最后一天日期时间
# last = now_first_day-datetime.timedelta(days=1)
# last_day = last.strftime("%Y-%m-%d")
