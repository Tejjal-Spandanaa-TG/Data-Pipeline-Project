import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO, BytesIO
import zipfile
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
import re
from datetime import datetime
import shutil
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Numeric, DateTime, text
import imaplib
import glob
import requests
import json
import smtplib
from email.mime.text import MIMEText

def format_column_name(column_name):
    formatted_name = column_name.lower().replace(' ', '_')
    formatted_name = ''.join(e for e in formatted_name if e.isalnum() or e == '_')
    return formatted_name

def create_table_query(engine, table_name):
    # Initialize metadata object
    metadata = MetaData()

    customer_details_temp = Table(
        table_name,
        metadata,
        Column('customer_id', Integer, primary_key=True),
        Column('salutation', String(10)),
        Column('name', String(100)),
        Column('name_on_the_card', String(100)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('pincode', String(10)),
        Column('district', String(50)),
        Column('office_ph_no', String(15)),
        Column('fax_number', String(15)),
        Column('mobile_no', String(20)),
        Column('email_id', String(100)),
        Column('address_1', Text),
        Column('address_2', Text),
        Column('dealer_code', String(20)),
        Column('al_dealer_code', String(20)),
        Column('dlr_sales_executive', String(100)),
        Column('no_of_cards', Integer),
        Column('drivinglicenseno', String(50)),
        Column('vehicle_regn_certificate', String(50)),
        Column('createddate', DateTime),
        Column('createdby', String(100)),
        Column('virtual_account_no', String(50)),
        Column('income_tax_pan', String(50))
    )

    metadata.create_all(engine)
    return customer_details_temp

def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    try:
        with smtplib.SMTP('mail.letsgro.co', 587) as smtp_server:
            smtp_server.starttls()
            smtp_server.login(sender, password)
            smtp_server.sendmail(sender, recipients, msg.as_string())
            print("Message sent!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def main():
    # Email credentials
    username = 'assume filled'
    password = 'assume filled'

    # Connect to the server
    mail = imaplib.IMAP4_SSL("assume filled")
    print(mail)

    # Login to your account
    mail.login(username, password)

    # Select the mailbox you want to search in
    mail.select("inbox")

    # Search for specific emails
    status, messages = mail.search(None, 'SUBJECT "assume filled"')
    print(status)
    print(messages)

    # Convert messages to a list of email IDs
    email_ids = messages[0].split()

    # Directory to save attachments
    download_dir = r"C:\Users\rmk spandanaa\Desktop\project"

    # Read the CSV file using pandas
    for file in os.listdir(download_dir):
        if file.endswith(".csv"):
            csv_file_path = os.path.join(download_dir, file)
            df = pd.read_csv(csv_file_path, low_memory=False)
            print(df.head())
            df.columns = [format_column_name(col) for col in df.columns]

            # Database connection parameters
            db_params = {
                'dbname': 'sample',
                'user': 'postgres',
                'password': 'postgres',
                'host': 'localhost',
                'port': '5432'  # Default PostgreSQL port is 5432
            }

            # Establish the connection
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()
            engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

            table_name = 'customer_details_temp'
            customer_details_temp = create_table_query(engine, table_name)

            data = df.to_dict('records')

            insert_query = text('''
                INSERT INTO customer_details_temp(
                    customer_id, salutation, name, name_on_the_card, city, state, pincode, district, office_ph_no,
                    fax_number, mobile_no, email_id, address_1, address_2, dealer_code, al_dealer_code, dlr_sales_executive,
                    no_of_cards, drivinglicenseno, vehicle_regn_certificate, createddate, createdby, virtual_account_no, income_tax_pan
                ) VALUES (
                    :customer_id, :salutation, :name, :name_on_the_card, :city, :state, :pincode, :district, :office_ph_no,
                    :fax_number, :mobile_no, :email_id, :address_1, :address_2, :dealer_code, :al_dealer_code, :dlr_sales_executive,
                    :no_of_cards, :drivinglicenseno, :vehicle_regn_certificate, :createddate, :createdby, :virtual_account_no, :income_tax_pan
                )
                ON CONFLICT (customer_id) DO UPDATE SET
                    salutation = excluded.salutation,
                    name = excluded.name,
                    name_on_the_card = excluded.name_on_the_card,
                    city = excluded.city,
                    state = excluded.state,
                    pincode = excluded.pincode,
                    district = excluded.district,
                    office_ph_no = excluded.office_ph_no,
                    fax_number = excluded.fax_number,
                    mobile_no = excluded.mobile_no,
                    email_id = excluded.email_id,
                    address_1 = excluded.address_1,
                    address_2 = excluded.address_2,
                    dealer_code = excluded.dealer_code,
                    al_dealer_code = excluded.al_dealer_code,
                    dlr_sales_executive = excluded.dlr_sales_executive,
                    no_of_cards = excluded.no_of_cards,
                    drivinglicenseno = excluded.drivinglicenseno,
                    vehicle_regn_certificate = excluded.vehicle_regn_certificate,
                    createddate = excluded.createddate,
                    createdby = excluded.createdby,
                    virtual_account_no = excluded.virtual_account_no,
                    income_tax_pan = excluded.income_tax_pan
            ''')

            total_inserted = 0
            with engine.begin() as connection:
                for i in range(0, len(data), 1000):  # Adjust batch size as needed
                    batch = data[i:i+1000]
                    result = connection.execute(insert_query, batch)
                    total_inserted += result.rowcount

            # Select the entire table and send it as an email attachment
            select_query = f"SELECT * FROM {table_name};"
            df_table = pd.read_sql_query(select_query, engine)

            # Email details
            subject = "Customer Details Report"
            body = "Please find the attached customer details report."
            sender = "assume filled"
            recipients = ["assume filled", "assume filled"]
            g_password = "assume filled"

            # Send the email with the table as an attachment
            send_email(subject, body, sender, recipients, g_password)

            # Close the connection
            engine.dispose()

if __name__ == "__main__":
    main()
