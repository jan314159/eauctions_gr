import io

import pandas as pd
import datetime
from typing import Optional

import bs4
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver import Chrome

from fake_useragent import UserAgent

import time

import numpy as np

class DownloadAllResults:
    def __init__(
            self,
            from_date,
            to_date,
            auction_df :pd.DataFrame,
            url="https://www.eauction.gr/en/Home/HlektronikoiPleistiriasmoi",
            max_page: Optional[int] = 1,
            asc=True):

        self.url = f"{url}?conductFrom={from_date.strftime('%d/%m/%Y')}&conductTo={to_date.strftime('%d/%m/%Y')}&sortAsc={asc}&sortId=1&conductedSubTypeId=1"
        self.auction_df = auction_df

        if max_page is None:
            page = self.download_page(page_no=1)
            self.max_page = int(page.find(class_="AList-GridPageCurrent").text.split("of")[1])
        else:
            self.max_page = max_page
        print(f"No of pages: {self.max_page}")

    def download_page(self, page_no: int = 1) -> BeautifulSoup:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")

        options.add_argument("--disable-blink-features=AutomationControlled")

        ua = UserAgent()
        userAgent = ua.random
        options.add_argument('user-agent={userAgent}')

        driver = Chrome(options=options)

        driver.get(f"{self.url}&page={page_no}")
        time.sleep(3)
        print(driver.current_url)

        soup_page = BeautifulSoup(driver.page_source, 'html.parser')

        # driver.close()
        driver.quit()

        return soup_page

    @staticmethod
    def extract_auction_info(tag: bs4.element.Tag) -> (str, str):
        try:
            params = tag.find(class_="AList-BoxMainCell4").text.replace('\xa0', '').replace('\n', '').strip().split("Region:")

            object_info = params[0].split(":")[1].strip()
            regional_info = params[1].strip()
        except AttributeError:
            object_info = "n/a"
            regional_info = "n/a"
        except IndexError:
            object_info = "n/a"
            regional_info = "n/a"

        return object_info, regional_info

    @staticmethod
    def extract_auction_posting(tag: bs4.element.Tag) -> (str, str, str):
        try:
            params = tag.find(class_="AList-BoxFooterLeft").text.replace('\xa0', '').replace('\n', '').strip().split(
                "Unique Code:")

            date_of_posting = params[0].split(":")[1].strip()
            auction_code = params[1].strip().split("Member of auction")
            if len(auction_code) == 2:
                unique_code = auction_code[0]
                member_of_auction = auction_code[1]
            else:
                unique_code = auction_code[0]
                member_of_auction = "n/a"
        except AttributeError:
            date_of_posting = "n/a"
            unique_code = "n/a"
            member_of_auction = "n/a"

        return date_of_posting, unique_code, member_of_auction

    @staticmethod
    def feature_class_extractor(tag: bs4.element.Tag, class_: str) -> str:
        try:
            r = tag.find(class_=class_).text.replace('\xa0', '').replace('\n', '').strip()
        except AttributeError:
            r = "n/a"
        return r


    def extract_info_about_listing(self, tag: bs4.element.Tag):
        # todo: toto tento split cathni nejaky error, ked tam nahodou nebude dvojbodka
        status = self.feature_class_extractor(tag, class_="AList-BoxheaderLeft").split(":")
        price = self.feature_class_extractor(tag, class_="AList-BoxTextPrice")
        debtor = self.feature_class_extractor(tag, class_="AList-BoxMainCell3").split(":")

        auction_date = self.feature_class_extractor(tag, class_="DateIcon")
        auction_time = self.feature_class_extractor(tag, class_="TimeIcon")

        auction_info = self.extract_auction_info(tag)
        auction_posting = self.extract_auction_posting(tag)

        hyperlink = tag.find('a', class_='AList-BoxFooterMore')['href']

        auction_json = {
            status[0]: status[1],
            "starting_bid": price,
            debtor[0]: debtor[1],
            "auction_date": auction_date,
            "auction_time": auction_time,
            "object_to_be_auctioned": auction_info[0],
            "regional_unit": auction_info[1],
            "date_of_posting": auction_posting[0],
            "unique_code": auction_posting[1],
            "member_of_auction": auction_posting[2],
            "link": hyperlink

        }

        return auction_json

    def __call__(self, *args, **kwargs):

        for page_no in range(1, self.max_page+1):
            pass




