#!/usr/bin/env python 

from selenium import webdriver
from bs4 import BeautifulSoup
import requests

driver = webdriver.Chrome('/usr/local/bin/chromedriver')
#driver.implicitly_wait(3)
ret = driver.get('https://diamond-e.kr/login')

driver.find_element_by_name('loginEmail').send_keys('idaeho@gmail.com')
driver.find_element_by_name('password').send_keys('xxxxxxxxxxxxx')
ret = driver.find_element_by_id('btnLogin').click()

#ret = driver.get('https://diamond-e.kr/product')
#ret = driver.get('https://diamond-e.kr/product/7f3ce9b0-63aa-11ea-9dc1-b4969166fed2')
#ret = driver.find_element_by_id('btnBuy').click()

#ret = driver.get('https://diamond-e.kr/order')
#ret = driver.find_element_by_id('agreeTerms').click()
#ret = driver.find_element_by_id('btnPayment').click()

#html = driver.page_source
#soup = BeautifulSoup(html, 'html.parser')
#notices = soup.select('div.p_inr >  div.p_info > a > span')
#for n in notices:
#    print(n.text.strip())

# Stop Program
driver.service.stop()

