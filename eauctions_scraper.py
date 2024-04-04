import itertools
import os
import time
from typing import Optional, List, Dict
from datetime import date
import datetime
import io

from selenium import webdriver
from selenium.webdriver import Chrome
from fake_useragent import UserAgent

import bs4
from bs4 import BeautifulSoup

import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


class GrAuctionsScraper:
    def __init__(self,
                 from_date,
                 to_date,
                 url="https://www.eauction.gr/en/Home/HlektronikoiPleistiriasmoi",
                 min_page: int = 1,
                 max_page: Optional[int] = 1,
                 asc=True):
        self.url = f"{url}?postFrom={from_date.strftime('%d/%m/%Y')}&postTo={to_date.strftime('%d/%m/%Y')}&sortAsc={asc}&sortId=1&conductedSubTypeId=1"
        self.min_page = min_page
        if max_page is None:
            page = self.download_page(page_no=1)
            self.max_page = int(page.find(class_="AList-GridPageCurrent").text.split("of")[1])
        else:
            self.max_page = max_page

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
    def feature_class_extractor(tag: bs4.element.Tag, class_: str) -> str:
        try:
            r = tag.find(class_=class_).text.replace('\xa0', '').replace('\n', '').strip()
        except AttributeError:
            r = "n/a"
        return r

    @staticmethod
    def extract_auction_info(tag: bs4.element.Tag) -> (str, str):
        try:
            params = tag.find(class_="AList-BoxMainCell4").text.replace('\xa0', '').replace('\n', '').strip().split(
                "Regional Unit:")

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
    def extract_auction_posting(tag: bs4.element.Tag) -> (str, str):
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

    def parse_all_listings_on_page(self, page: BeautifulSoup) -> List[Dict[str, str]]:
        l = []

        re_ = page.find_all(class_="AList-BoxContainer")

        for re in re_:
            l.append(self.extract_info_about_listing(re))

        return l

    def parse_page(self, page_no=1):

        if (page_no > 1) & (page_no % 10 == 0):
            time.sleep(60 * 2.5)
            print(f"sleeping, scraped more than 10 euactions pages")

        page = self.download_page(page_no=page_no)
        page_l = self.parse_all_listings_on_page(page)

        return page_l

    @staticmethod
    def flatten_list(nested_list):
        return list(itertools.chain(*nested_list))

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        r = list(
            self.flatten_list(
                list(
                    map(self.parse_page, range(self.min_page, self.max_page + 1))
                )
            )
        )

        return pd.DataFrame(r)


class SingleListingParsing:
    def __init__(self, auctions_df: pd.DataFrame):
        self.auctions_df = auctions_df

    @staticmethod
    def download_page(url: str) -> BeautifulSoup:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")

        options.add_argument("--disable-blink-features=AutomationControlled")

        ua = UserAgent()
        userAgent = ua.random
        options.add_argument('user-agent={userAgent}')

        driver = Chrome(options=options)

        driver.get(url)
        time.sleep(3)
        print(driver.current_url)

        soup_page = BeautifulSoup(driver.page_source, 'html.parser')

        driver.quit()
        #     driver.close()

        return soup_page

    @staticmethod
    def get_single_page_params(page, row=None):
        auction_params = page.find_all(class_="AuctionDetailsDivR")
        auction_descs = page.find_all(class_="AuctionDetailsDiv")

        for param in auction_params:
            if param.find("label").text.strip() == "Debtors' Vat Numbers":
                debtor_vat = list(map(lambda x: x.text.strip(), param.find_all("label", class_="ADetailsinput")))

            elif param.find("label").text.strip() == 'Debtor`s VAT Number':
                debtor_vat = list(map(lambda x: x.text.strip(), param.find_all("label", class_="ADetailsinput")))

        for desc in auction_descs:
            name = desc.find("label").text.strip()
            if name == "Debtors' Names and Surnames":
                debtor_name = list(map(lambda x: x.text.strip(), desc.find_all("label", class_="ADetailsinput3Cell")))

            elif name == "Debtor`s Name and Surname":
                debtor_name = list(map(lambda x: x.text.strip(), desc.find_all("label", class_="ADetailsinput3Cell")))

            elif name == "Date of Conduction":
                date_of_conduct = desc.find("label", class_="ADetailsinputDateOn").text.strip()

            elif name == "Unique Code":
                unique_code = desc.find("label", class_="ADetailsinput").text.strip()

            elif name == "Hastener":
                hastener_name = desc.find("label", class_="ADetailsinput3Cell").text.strip()

        if len(debtor_name) == len(debtor_vat):
            auctions_params = []
            for i in range(len(debtor_name)):
                if row is not None:
                    a = {
                        "debtor_name": debtor_name[i],
                        "debtor_vat": debtor_vat[i],
                        "date_of_conduct": date_of_conduct,
                        "unique_code_1": unique_code,
                        "hastener_name": hastener_name,
                        'Status': row['Status'],
                        'starting_bid': row['starting_bid'],
                        'Debtor': row['Debtor'],
                        'auction_date': row['auction_date'],
                        'auction_time': row['auction_time'],
                        'object_to_be_auctioned': row['object_to_be_auctioned'],
                        'regional_unit': row['regional_unit'],
                        'date_of_posting': row['date_of_posting'],
                        'unique_code': row['unique_code'],
                        'member_of_auction': row["member_of_auction"],
                        'link': row['link']
                    }
                else:
                    a = {
                        "debtor_name": debtor_name[i],
                        "debtor_vat": debtor_vat[i],
                        "date_of_conduct": date_of_conduct,
                        "unique_code_1": unique_code,
                        "hastener_name": hastener_name}

                auctions_params.append(a)

        return auctions_params

    @staticmethod
    def flatten_list(nested_list):
        return list(itertools.chain(*nested_list))

    def __call__(self, *args, **kwargs):
        t = []

        no_of_listings = df.shape[0]

        for i, row in df.iterrows():

            print(f"Scraping page {i} out of {no_of_listings}")

            if i % 10 == 0:
                print("sleeping")
                time.sleep(60 * 2.5)

            page = self.download_page(row["link"])
            try:
                row_params = self.get_single_page_params(page, row)
            except UnboundLocalError:
                row_params = [{
                    "debtor_name": "please check manually",
                    "debtor_vat": "n/a",
                    "date_of_conduct": "n/a",
                    "unique_code_1": "n/a",
                    "hastener_name": "n/a",
                    'Status': row['Status'],
                    'starting_bid': row['starting_bid'],
                    'Debtor': row['Debtor'],
                    'auction_date': row['auction_date'],
                    'auction_time': row['auction_time'],
                    'object_to_be_auctioned': row['object_to_be_auctioned'],
                    'regional_unit': row['regional_unit'],
                    'date_of_posting': row['date_of_posting'],
                    'unique_code': row['unique_code'],
                    'member_of_auction': row["member_of_auction"],
                    'link': row['link']
                }]
            t.append(row_params)

        return pd.DataFrame(self.flatten_list(t))


def send_email(attachment,
               to_date,
               auction_params_dict: dict,
               sender_email="test.mail.data.scrp2024@gmail.com",
               sender_password="cjbo nywb zhaa gywm",
               recipient_email="janpitonak.jp@gmail.com") -> None:
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email.strip()
    msg['Subject'] = f"GR auction data {date.today()}"

    name = recipient_email.split("@")[0].split(".")

    if len(name) == 1:
        recipient = f"{name[0].capitalize()}"
    else:
        recipient = f"{name[0].capitalize()} {name[1].capitalize()}"

    body = f"""
        Dear {recipient},\n
    
        
        please find the new listing from eauctions.gr from date: {to_date}. (in case it's weekend, it also includes listings from
        Friday and Saturday.)\n
        There were added {auction_params_dict["no_of_all_listings"]} auctions of which\n
        Frame auctions: {auction_params_dict["frame_listings"]} 
        w/ {auction_params_dict["frame_unique_debtors"]} unique debtors 
        and first auction is held on {auction_params_dict["frame_first_auction"]}\n
        Arctos auctions: {auction_params_dict["arctos_listings"]} 
        w/ {auction_params_dict["arctos_unique_debtors"]} unique debtors 
        and first auction is held on {auction_params_dict["arctos_first_auction"]}\n
        
        Also please note that there are {auction_params_dict["manual_check"]} auctions that has to be manually checked.
        
        Please do not response to this mail, in case of any questions or requires please write directly to jan.pitonak@aps-holding.com
        """
    msg.attach(MIMEText(body, 'plain'))

    part1 = MIMEApplication(attachment)
    part1.add_header('Content-Disposition', 'attachment', filename=f"eauctions_gr_{to_date}.xlsx")
    msg.attach(part1)

    #
    # part2 = MIMEApplication(attachment_arctos)
    # part2.add_header('Content-Disposition', 'attachment', filename=f"arctos_auctions_{to_date}.xlsx")
    # msg.attach(part2)
    #
    # part3 = MIMEApplication(attachment_frame)
    # part3.add_header('Content-Disposition', 'attachment', filename=f"frame_auctions_{to_date}.xlsx")
    # msg.attach(part3)
    #
    # part4 = MIMEApplication(attachment_manual_check)
    # part4.add_header('Content-Disposition', 'attachment', filename=f"to_manual_check_{to_date}.xlsx")
    # msg.attach(part4)

    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()

    server.login(sender_email, sender_password)

    server.sendmail(sender_email, recipient_email, msg.as_string())

    server.quit()


def get_our_debtors(listings_df: pd.DataFrame, debtos_df: pd.DataFrame, listing_vat_column: str,
                    debors_vat_column: str):
    debtos_df[debors_vat_column] = pd.to_numeric(debtos_df[debors_vat_column], errors="coerce")
    debtos_df.dropna(subset=[debors_vat_column], inplace=True)
    debtos_df[debors_vat_column] = debtos_df[debors_vat_column].apply(int)

    listings_df[listing_vat_column] = pd.to_numeric(listings_df[listing_vat_column], errors="coerce")

    return listings_df.merge(debtos_df, left_on=listing_vat_column, right_on=debors_vat_column)


def get_dates():
    today = datetime.date.today()
    to_date = today - datetime.timedelta(days=1)

    if today.weekday() == 0:
        from_date = today - datetime.timedelta(days=3)
    else:
        from_date = today - datetime.timedelta(days=1)
    return from_date, to_date


def write_multiple_sheet_excel(buffer: io.BytesIO,
                               all_listings: pd.DataFrame,
                               frame: pd.DataFrame,
                               arctos: pd.DataFrame,
                               manual_check: pd.DataFrame):
    with pd.ExcelWriter(buffer, mode="w", engine='xlsxwriter') as write:
        workbook = write.book

        workbook.formats[0].set_font_name("Arial")
        workbook.formats[0].set_font_size(9)

        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'fg_color': '#005864',
            'border': 0,
            'color': 'white',
            'font': 'Arial',
            'font_size': 9})

        # sheet all listings

        all_listings.to_excel(write, sheet_name="All listings", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["All listings"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(all_listings.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        # sheet frame

        frame.to_excel(write, sheet_name="Frame", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["Frame"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(frame.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        # sheet arctos

        arctos.to_excel(write, sheet_name="Arctos", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["Arctos"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(arctos.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        # sheet Manual check

        manual_check.to_excel(write, sheet_name="To manual check", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["To manual check"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(manual_check.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)


if __name__ == "__main__":
    from_date, to_date = get_dates()

    print(f"from date: {from_date}\nto date: {to_date}")
    scraper = GrAuctionsScraper(from_date=from_date, to_date=to_date, max_page=None)
    df = scraper()
    print(f"no of listings: {df.shape[0]}")
    # df.to_excel(f"gr_auctions_{to_date}.xlsx", index=False)

    single_parser = SingleListingParsing(df)
    single_listings_df = single_parser()
    # single_listings_df.to_excel(f"all_listings_{to_date}.xlsx", index=False)
    # single_listings_df[single_listings_df["debtor_name"] == "please check manually"].to_excel(f"to_manual_check_{to_date}.xlsx", index=False)

    # single_listings_df = pd.to_excel(f"all_listings_{date.today()}.xlsx")

    frame_df = pd.read_excel("FRAME Borrowers.xlsx")
    frame_auctions = get_our_debtors(single_listings_df, frame_df, listing_vat_column="debtor_vat",
                                     debors_vat_column="VAT Number")
    # frame_auctions.to_excel(f"frame_auctions_{to_date}.xlsx", index=False)

    arctos_df = pd.read_excel("ARCTOS Borrowers.xlsx")
    arctos_auctions = get_our_debtors(single_listings_df, arctos_df, listing_vat_column="debtor_vat",
                                      debors_vat_column="VAT Number")

    buffer = io.BytesIO()

    write_multiple_sheet_excel(
        buffer=buffer,
        all_listings=single_listings_df,
        frame=frame_auctions,
        arctos=arctos_auctions,
        manual_check=single_listings_df[single_listings_df["debtor_name"] == "please check manually"])

    auction_params_dict = {
        "no_of_all_listings": single_listings_df.shape[0],
        "frame_listings": frame_auctions.shape[0],
        "frame_unique_debtors": frame_auctions["debtor_vat"].drop_duplicates().shape[0],
        "frame_first_auction": str(pd.to_datetime(frame_auctions["date_of_conduct"], dayfirst=True).min()),
        "arctos_listings": arctos_auctions.shape[0],
        "arctos_unique_debtors": arctos_auctions["debtor_vat"].drop_duplicates().shape[0],
        "arctos_first_auction": str(pd.to_datetime(arctos_auctions["date_of_conduct"], dayfirst=True).min()),
        "manual_check": single_listings_df[single_listings_df["debtor_name"] == "please check manually"].shape[0]
    }

    # arctos_auctions.to_excel(f"arctos_auctions_{to_date}.xlsx", index=False)

    # attachment = open(f"gr_auctions_{date.today()}.xlsx", "rb").read()

    # attachment_all_listings = open(f"all_listings_{to_date}.xlsx", "rb").read()
    # attachment_arctos = open(f"arctos_auctions_{to_date}.xlsx", "rb").read()
    # attachment_frame = open(f"frame_auctions_{to_date}.xlsx", "rb").read()
    # attachment_manual_check = open(f"to_manual_check_{to_date}.xlsx", "rb").read()
    # send_email(attachment)

    path_to_email_list = "participants_emails.txt"
    with open(path_to_email_list, 'r') as file:
        # Read the entire content of the file
        emails = file.read()

    emails_list = emails.split("\n")
    for email in emails_list:
        send_email(buffer.getvalue(), auction_params_dict=auction_params_dict,
                   to_date=to_date, recipient_email=email)

    # os.remove(f"all_listings_{to_date}.xlsx")
    # os.remove(f"arctos_auctions_{to_date}.xlsx")
    # os.remove(f"frame_auctions_{to_date}.xlsx")
    # os.remove(f"to_manual_check_{to_date}.xlsx")
