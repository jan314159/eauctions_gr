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

class GetAuctionResults:
    def __init__(self, auctions_df: pd.DataFrame):
        self.df = auctions_df

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
    def extract_params(page):
        params = page.find(class_="StateBox")

        labels = params.find_all(class_="Statelabel")
        values = params.find_all(class_="StateValue")

        r = {}

        for i in range(len(labels)):
            r[labels[i].text.strip()] = values[i].text.strip()

        return r

    @staticmethod
    def convert_to_val(s):

        if isinstance(s, str):
            try:
                r = float(s.replace("€", "").replace(".", "").replace(",", "."))

            except ValueError:
                r = -1

        elif isinstance(s, float):
            r = s

        else:
            r = -1

        return r


    def get_prices(self, row):
        min_bid = self.convert_to_val(row["starting_bid"])
        award = self.convert_to_val(row["Award ammount:"])

        return min_bid, award

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        res = []

        for i, row in self.df.iterrows():

            if i % 10 == 0:
                print("sleeping")
                time.sleep(60 * 2.5)

            page = self.download_page(row["link"])

            try:
                auctions_status = self.extract_params(page)
            except:
                auctions_status = {"error": "n/a"}


            res.append({**row.to_dict(), **auctions_status})

        res_df = pd.DataFrame(res)

        res_df["min_bid"] = res_df["starting_bid"].apply(self.convert_to_val)

        if "Award ammount:" in res_df.columns:
            res_df["award"] = res_df["Award ammount:"].apply(self.convert_to_val)
        else:
            res_df["award"] = np.NaN

        return res_df

def create_pivot(df, index_col):
    return df.fillna(0).pivot_table(
        index=index_col,
        values=["debtor_name", "min_bid","award"],
        aggfunc={
            "debtor_name": "count",
            "award": "sum",
            "min_bid": "mean"},
        margins=True,
        margins_name="Total"
    ).rename(
        columns={"debtor_name": "no of auctions", "min_bid": "avg starting bid"})

def aucion_results(
        buffer: io.BytesIO,
        frame_results: pd.DataFrame,
        arctos_results: pd.DataFrame):
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

        table_names_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'border': 0,
            'color': '#005864',
            'font': 'Arial',
            'font_size': 9})

        strats_index_format = workbook.add_format({
            'bold': False,
            'text_wrap': False,
            'valign': 'top',
            'border': 0,
            'font': 'Arial',
            'font_size': 9})

        strats_index_format_date = workbook.add_format({
            'bold': False,
            'text_wrap': False,
            'valign': 'top',
            'border': 0,
            'font': 'Arial',
            'font_size': 9, 'num_format': 'd-mmmm-yy'})

        strats_total_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'fg_color': '#E83E33',
            'border': 0,
            'color': 'white',
            'font': 'Arial',
            'font_size': 9, 'num_format': '# ##0'})

        strats_header_format = workbook.add_format({
            'bold': True,
            'text_wrap': False,
            'valign': 'top',
            'fg_color': '#468E99',
            'border': 0,
            'color': 'white',
            'font': 'Arial',
            'font_size': 9})

        number_format = workbook.add_format(
            {'num_format': '# ##0'}
        )

        # sheet frame

        frame_results.to_excel(write, sheet_name="Frame results", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["Frame results"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(frame_results.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        # sheet arctos

        arctos_results.to_excel(write, sheet_name="Arctos results", startrow=1, startcol=1, index=False)

        worksheet = write.sheets["Arctos results"]
        worksheet.hide_gridlines(2)

        for col_num, value in enumerate(arctos_results.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        # strats

        # ---- Frame ----

        start_row = 3
        start_col = 1

        frame_status = create_pivot(frame_results, "Status:")
        frame_status.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col)

        worksheet = write.sheets["Strats"]
        worksheet.hide_gridlines(2)

        worksheet.set_column(first_col=start_col + 1, last_col=start_col + 3, width=20, cell_format=number_format)
        worksheet.set_column(first_col=start_col, last_col=start_col, width=45)

        worksheet.write(1, 1, "Frame", table_names_format)

        for col_num, value in enumerate(["Status"] + frame_status.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + frame_status.loc["Total"].values.tolist()):
            worksheet.write(start_row + frame_status.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(frame_status.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        end_row = start_row + frame_status.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        frame_hastener = create_pivot(frame_results, "hastener_name")
        frame_hastener.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=1, )

        for col_num, value in enumerate(["Hastener name"] + frame_hastener.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + frame_hastener.loc["Total"].values.tolist()):
            worksheet.write(start_row + frame_hastener.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(frame_hastener.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        end_row = start_row + frame_hastener.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        frame_date = create_pivot(frame_results, "auction_date")
        frame_date.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col, )

        for col_num, value in enumerate(["Auction date"] + frame_date.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + frame_date.loc["Total"].values.tolist()):
            worksheet.write(start_row + frame_date.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(frame_date.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format_date)

        end_row = start_row + frame_date.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        frame_servicer = create_pivot(frame_results , "Servicer")
        frame_servicer.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col, )

        for col_num, value in enumerate(["Servicer"] + frame_servicer.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + frame_servicer.loc["Total"].values.tolist()):
            worksheet.write(start_row + frame_servicer.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(frame_servicer.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        end_row = start_row + frame_servicer.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        frame_manager = create_pivot(frame_results, "Case Manager")
        frame_manager.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col, )

        for col_num, value in enumerate(["Case Manager"] + frame_manager.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + frame_manager.loc["Total"].values.tolist()):
            worksheet.write(start_row + frame_manager.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(frame_manager.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        # ---- Arctos ----

        start_row = 3
        start_col = 7

        arctos_status = create_pivot(arctos_results, "Status:")
        arctos_status.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col)

        worksheet.set_column(first_col=start_col + 1, last_col=start_col + 3, width=20, cell_format=number_format)
        worksheet.set_column(first_col=start_col, last_col=start_col, width=45)

        start_row = 3

        worksheet.write(1, start_col, "Arctos", table_names_format)

        for col_num, value in enumerate(["Status"] + arctos_status.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + arctos_status.loc["Total"].values.tolist()):
            worksheet.write(start_row + arctos_status.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(arctos_status.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        end_row = start_row + arctos_status.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        arctos_hastener = create_pivot(arctos_results, "hastener_name")
        arctos_hastener.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col, )

        for col_num, value in enumerate(["Hastener name"] + arctos_hastener.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + arctos_hastener.loc["Total"].values.tolist()):
            worksheet.write(start_row + arctos_hastener.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(arctos_hastener.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format)

        end_row = start_row + arctos_hastener.shape[0]

        # ----------------------------------------------------

        start_row = end_row + 3

        arctos_date = create_pivot(arctos_results, "auction_date")
        arctos_date.to_excel(write, sheet_name="Strats", startrow=start_row, startcol=start_col, )

        for col_num, value in enumerate(["Auction date"] + arctos_date.columns.values.tolist()):
            worksheet.write(start_row, col_num + start_col, value, strats_header_format)

        for col_num, value in enumerate(['Total'] + arctos_date.loc["Total"].values.tolist()):
            worksheet.write(start_row + arctos_date.shape[0], col_num + start_col, value, strats_total_format)

        for row_num, value in enumerate(arctos_date.index.to_list()[:-1]):
            worksheet.write(start_row + 1 + row_num, start_col, value, strats_index_format_date)



