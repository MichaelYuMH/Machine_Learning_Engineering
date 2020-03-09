import re
import requests
from time import sleep
import unicodedata
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime

class SEC_crawler():
    def __init__(self, after_date):
        self._after_date = after_date
    
    def set_after_date(self, after_date):
        self._after_date = after_date
    
    def get_data(self, cik, ticker, print_process=False):
        if(print_process):
            print('Get document links......')
        df = self._get_doc_links(cik, ticker)
        
        if(print_process):
            print('Get exhibits links......')
        df = self._get_exhib_doc_links(df)
        
        if(print_process):
            print('Get report dates......')
        df = self._get_file_dates(df)
        
        return(df)
    
    def extract_exhibit_doc_text(self, link_list):
        if(len(link_list) == 0):
            return(None)

        result = ""

        for link in link_list:
            try:
                r = requests.get(link)
                #Parse 8-K document
                soup = BeautifulSoup(r.content,"html5lib",from_encoding="ascii")

                #Extract HTML sections
                for section in soup.findAll("html"):
                    try:
                        #Remove tables
                        for table in section("table"):
                            table.decompose()
                        #Convert to unicode
                        section = unicodedata.normalize("NFKD",section.text)
                        section = section.replace("\t"," ").replace("\n"," ").replace("/s"," ").replace("\'","'")            
                    except AttributeError:
                        section = str(section.encode('utf-8'))
                doc = "".join((section))

                result = result + ' ' + doc
            except requests.exceptions.ConnectionError:
                    sleep(10)
            sleep(.1)

        return(result)
    
    def _get_doc_links(self, cik, ticker):
        try:
            # Get html links
            html_list = []
            date_list = []

            h_list = [None] # To initial the While loop below, assign andom values 
            d_list = [None] #   so that the len of the lists would not be zero.

            while((len(d_list) != 0)):
                h_list, d_list = self._query_8K_files(cik=cik, after_date=self._after_date, start=len(html_list))

                html_list = html_list + h_list
                date_list = date_list + d_list
            
            doc_list = []
            doc_name_list = []

            # Get links txt files
            for i in html_list:
                txt_doc = i.replace('-index.html','.txt')
                doc_name = txt_doc.split('/')[-1]
                doc_list.append(txt_doc)
                doc_name_list.append(doc_name)

            df = pd.DataFrame(
                {
                    'cik': [cik] * len(html_list),
                    'ticker': [ticker] * len(html_list),
                    'txt_link': doc_list,
                    'doc_name': doc_name_list
                }
            )
        except:
            print('Failed')
            requests.exceptions.ConnectionError
            sleep(0.5)
        
        return(df)
    
    def _get_exhib_doc_links(self, df):
        zip_pair_list = self._get_txt_link_and_doc_name_pairs(df)
        result = pd.Series(zip_pair_list).apply(self._extract_exhib_doc_url)
        unzip_result = list(zip(*result))
        
        exhib_doc_url, item_no = unzip_result[0], unzip_result[1]
        
        df['exhibit_link'] = exhib_doc_url
        df['item_no'] = item_no
        
        return(df)
    
    def _get_file_dates(self, df):
        result = df['txt_link'].apply(self._extract_date)
        unzip_result = list(zip(*result))
        
        accepted_at, period_of_report = unzip_result[0], unzip_result[1]
        
        df['accepted_date'] = accepted_at
        df['period_of_report'] = period_of_report
        
        return(df)
    
    def _get_txt_link_and_doc_name_pairs(self, df):
        zip_pair_list = list(zip(df['txt_link'], df['doc_name']))
        
        return(zip_pair_list)
            
    def _query_8K_files(self, cik, after_date, count=100, start=0):
        url = 'https://www.sec.gov/cgi-bin/browse-edgar'
        payload = {
            'action': 'getcompany',
            'type': '8-K',
            'output': 'xml',
            'CIK': cik,
            'count': count,
            'start': start
        }

        response = requests.get(url=url, params=payload)
        soup = BeautifulSoup(response.text, 'lxml')
        url_list = soup.findAll('filinghref')
        filing_list = soup.findAll('datefiled')
        filing_list = [datetime.strptime(i.string, '%Y-%m-%d').date() for i in filing_list]

        html_list = []
        date_list = []

        for date, link in zip(filing_list, url_list):
            if(date > after_date):
                link = link.string
                if(link.split('.')[-1] == 'htm'):
                    html_list.append(link + 'l')
                    date_list.append(date)

        assert(len(html_list) == len(date_list))

        return(html_list, date_list)
    
    def _extract_exhib_doc_url(self, zip_pair):
        txt_link, doc_name =  zip_pair
        r = requests.get(txt_link.replace('.txt','-index.html'))
        soup = BeautifulSoup(r.content,"html5lib",from_encoding="ascii")

        doc_files_table = soup.find('table', {'class':'tableFile', 'summary':'Document Format Files'})
        df_table = pd.read_html(str(doc_files_table))[0]

        exhib_doc_name_list = df_table.loc[[str(t).find('EX') != -1 for t in df_table['Type'].values], 'Document'].values
        exhib_doc_url = [txt_link.replace(doc_name, i) for i in exhib_doc_name_list]
        
        item_no = self._extract_item_no(soup.text)

        return((exhib_doc_url, item_no))
    
    def _extract_item_no(self, document):
        pattern = re.compile("Item+ +\d+[\:,\.]+\d+\d")
        item_list = re.findall(pattern,document)
        return item_list
    
    def _extract_date(self, txt_link):
        try:
            r = requests.get(txt_link.replace('.txt','-index.html'))
            soup = BeautifulSoup(r.content,"html5lib",from_encoding="ascii")
            
            keyword_acpt = 'Accepted'
            keyword_por = 'Period of Report'
            
            accepted_at = self._find_date_by_key_word(soup.text, keyword_acpt, 10)
            period_of_report = self._find_date_by_key_word(soup.text, keyword_por, 10)
            
            return((accepted_at, period_of_report))
        
        except:
            print('Failed extracting dates')
            requests.exceptions.ConnectionError
            sleep(0.5)
            return((None, None))

    def _find_date_by_key_word(self, text, keyword, offset):
        index = text.find(keyword)
        date_format_len = 10 # 'YYYY-mm-dd'
        
        if(index != -1):
            date_str = text[(index+len(keyword)+offset):(index+len(keyword)+offset+date_format_len)]
            return(date_str)
        else:
            return(None)