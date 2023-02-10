import os
import traceback
from dotenv import load_dotenv
import time
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent
from category import naver_news_category
import telegram
import sys
sys.path.append("/dahy/company_analysis/")
from set_log import set_log
from database.insert_update import insert_or_update_news
from datetime import datetime


logger = set_log()

class CollectNews:

    def get_text(self, site:str, url:str):
        """
        site : 네이버, 다음 url을 바탕으로 뉴스기사의 제목과 본문을 return 하는 함수

        """

        ua = UserAgent()
        headers={"user-agent": ua.random }
        response=requests.get(url, headers=headers)
        time.sleep(3)
        if response.status_code == 200:
            pass
        else:
            time.sleep(3)


        if site == 'naver':
            try:

                soup2 = BeautifulSoup(response.content, "html.parser")
                title=soup2.select_one("#title_area > span").text
                text=soup2.select_one("#dic_area").text
                dt=soup2.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div:nth-child(1) > span")['data-date-time']

                datetime_format = "%Y-%m-%d %H:%M:%S"

                dt = datetime.strptime(dt, datetime_format)
                
                return dt, title, text

            except Exception as e:

                tb = traceback.format_exc()
                logger.error("get_text 함수 naver에서 에러발생\n error:{}\nTraceback: {}".format(e, tb))
                pass
        if site == 'daum':
            try:
                soup3 = BeautifulSoup(response.content, "html.parser")
                title=soup3.select_one("#mArticle > div.head_view > h3").text
                text=soup3.select_one("#mArticle > div.news_view.fs_type1 > div.article_view > section").text
                try:
                    dt=soup3.select_one("#mArticle > div.head_view > div.info_view > span:nth-child(2) > span").text
                except:
                    dt=soup3.select_one("#mArticle > div.head_view > div.info_view > span > span").text
                    
   

                
                datetime_format = "%Y. %m. %d. %H:%M"
                dt = datetime.strptime(dt, datetime_format)
                return dt, title, text
            except Exception as e:

                tb = traceback.format_exc()
                logger.error("get_text 함수 daum에서 에러발생\n error:{}\nTraceback: {}".format( e, tb))
                pass

            



    def collect_news_list(self, base_url: str,date,page: int) -> list:
        """
        특정 뉴스 카테고리의 링크들을 수집하는 함수
        base_url : ex) "https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1=100&sid2=264"
        date : "yyyymmdd"
        page : 1
        """
        base_url_date_page = base_url + f"&date={date}&page={page}"
        ua = UserAgent()
        headers={"user-agent": ua.random }
        response=requests.get(base_url_date_page, headers=headers)
        time.sleep(3)
        soup = BeautifulSoup(response.content, "html.parser")

        news_body=soup.select_one("#main_content > div.list_body.newsflash_body")
        news_href_list=list(set([a["href"] for  a in news_body.find_all("a")]))
        return news_href_list


    def collect_naver(self, cat_num: int):
        """
        cat =>
        0: 정치
        1: 경제
        2: 사회
        3: 생활문화
        4: IT과학세계
        """
        
        import datetime
        user = os.environ.get('username')
        password = os.environ.get('password')
        host = os.environ.get('host')
        database ='news_db'
        try:
            date=datetime.datetime.today().date().strftime("%Y%m%d")
            page=1
            site='naver'

            for base_url in tqdm(naver_news_category[cat_num].keys()):
                news_href_list=self.collect_news_list(base_url,date,page)
                category=naver_news_category[cat_num][base_url]
                
                for url in news_href_list:

                    dt, title, text= self.get_text(site,url)
                    text=text.replace("\n","").replace("\t","").strip()

                    df_temp=pd.DataFrame({"datetime": [dt],"category":[category], "title":[title],"text":[text], "link":[url]})
                    print(df_temp.columns)
                    insert_or_update_news(site, df_temp, host, user, password, database)
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"collect_naver 에러발생 {e}\n{tb}")

    def collect_daum(self):
        """
        다음 최신 뉴스 15개씩 수집하여 DataFrame으로 반환하는 함수

        """
        user = os.environ.get('username')
        password = os.environ.get('password')
        host = os.environ.get('host')
        database ='news_db'
        site='daum'
        
        
        ua = UserAgent()
        headers={"user-agent": ua.random }
        url='https://news.daum.net/breakingnews?page=1'
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text,  "html.parser")
        urls=list(map(lambda x: x.attrs['href'] , soup.select('a.link_txt')))


        for url in urls[0:15]:
            try:
                dt, title, text=self.get_text(site, url)
                
                text=text.replace("\n","").replace("\t","").strip()
                df_temp=pd.DataFrame({'datetime':[dt], "title":[title],"text":[text], "link":[url]})
                
                insert_or_update_news(site, df_temp, host, user, password, database)
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(f"collect_daum 에러발생 {e} \n{tb}")
         


