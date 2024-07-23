import argparse
import datetime
import io

import pandas as pd

from eauctions_scraper import get_table_from_sql_db, get_dates, GrAuctionsScraper, SingleListingParsing, \
    get_our_debtors, convert_date, upload_data, ExcelWriterGR, send_email_multiple_borrowers

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--password", "-p", type=str, required=True)
    parser.add_argument("--database", "-db", type=str, required=True)
    parser.add_argument("--username", "-u", type=str, required=True)
    parser.add_argument("--server", "-s", type=str, required=True)

    parser.add_argument("--borrowers_tables", "-b", nargs="*", required=True, default=[])
    parser.add_argument("--mailing_list", "-ml", type=str, required=True)

    parser.add_argument("--max_page", "-max", type=int, required=False, default=None)
    parser.add_argument("--sender_password", "-sp", type=str, required=True)

    parser.add_argument("--auction_tables", "-a", nargs="*", required=True, default=[])

    args = parser.parse_args()

    borrowers = []
    try:
        for borrower in args.borrowers_tables:
            borrowers.append(
                get_table_from_sql_db(table_name=borrower, db=args.database, username=args.username,
                                      pswrd=args.password, server=args.server))
    except:
        for borrower in ["FRAME Borrowers.xlsx", "ARCTOS Borrowers.xlsx", "HOLIDAY Borrowers.xlsx"]:
            borrowers.append(pd.read_excel(borrower))

    try:
        mailing_list = get_table_from_sql_db(table_name=args.mailing_list, db=args.database, username=args.username,
                                         pswrd=args.password, server=args.server)
    except:
        mailing_list = pd.read_fwf('participants_emails.txt')

    emails_list = mailing_list["email"].to_list()
    from_date, to_date = get_dates()

    # from_date = datetime.date.today() - datetime.timedelta(days=4)

    print(f"from date: {from_date}\nto date: {to_date}")
    scraper = GrAuctionsScraper(from_date=from_date, to_date=to_date, max_page=args.max_page)
    df = scraper()
    print(f"no of listings: {df.shape[0]}")

    single_parser = SingleListingParsing(df)
    single_listings_df = single_parser()

    borrowers_auctions = []
    table_no = 0
    for borrower_df in borrowers:
        borrower_auctions = get_our_debtors(single_listings_df, borrower_df,
                                            listing_vat_column="debtor_vat", debors_vat_column="VAT Number")
        borrower_auctions["auction_date"] = borrower_auctions["auction_date"].apply(convert_date)
        try:
            upload_data(borrower_auctions, table_name=args.auction_tables[table_no], pswrd=args.password)
        except:
            borrower_auctions.to_excel(f"{args.auction_tables[table_no]}_{to_date}.xlsx")

        table_no += 1

        borrowers_auctions.append(borrower_auctions)

    buffer = io.BytesIO()

    excel_writer = ExcelWriterGR(
        buffer=buffer,
        all_listings=single_listings_df,
        borrowers_listings=borrowers_auctions,
        manual_check=single_listings_df[single_listings_df["debtor_name"] == "please check manually"],
        sheet_names=args.borrowers_tables)

    excel_writer()

    for email in emails_list:
        send_email_multiple_borrowers(
            buffer.getvalue(),
            to_date=to_date,
            borrowers_auctions=borrowers_auctions,
            borrwers_names=args.borrowers_tables,
            sender_password=args.sender_password,
            manual_check=single_listings_df[single_listings_df["debtor_name"] == "please check manually"],
            recipient_email=email)


