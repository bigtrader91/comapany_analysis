import mysql.connector
import sys
import os
sys.path.append("/dahy/company_analysis/")
from set_log import set_log

current_directory = os.getcwd()

# 데이터베이스 연결 생성
def insert_or_update_news(site, df, host, user, password, database):
    logger=set_log()
    # Connect to the MySQL database
    con = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = con.cursor()
    cursor.execute("START TRANSACTION")

    try:
        if site == 'naver':
            logger.info(f"naver에서 수집을 시작합니다.")
            # 반복문을 돌면서 데이터프레임의 row 를 데이터베이스 insert or update
            for index, row in df.iterrows():
                sql = f"SELECT * FROM news_{site} WHERE title = %s AND link = %s"
                cursor.execute(sql, (row['title'], row['link']))
                result = cursor.fetchone()
                if result:
                    sql = f"UPDATE news_{site} SET datetime = %s, category = %s, text = %s  WHERE title = %s AND link = %s"
                    cursor.execute(sql, (row['datetime'] ,  row['category'], row['title'],  row['text'],  row['link']))
                    logger.info(f"데이터가 정상적으로 업데이트되었습니다.")
                else:
                    sql = f"INSERT INTO news_{site} (datetime,category, title, text,  link) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (row['datetime'] ,  row['category'], row['title'],  row['text'],  row['link']))
                    logger.info(f"데이터가 정상적으로 추가되었습니다.")
        elif site == 'daum':

            logger.info(f"daum에서 수집을 시작합니다.")
            # 반복문을 돌면서 데이터프레임의 row 를 데이터베이스 insert or update
            for index, row in df.iterrows():
                sql = f"SELECT * FROM news_{site} WHERE title = %s AND link = %s"
                cursor.execute(sql, (row['title'], row['link']))
                result = cursor.fetchone()
                if result:
                    sql = f"UPDATE news_{site} SET datetime = %s, text = %s WHERE title = %s AND link = %s"
                    cursor.execute(sql, (row['datetime'] , row['title'],  row['text'],  row['link']))
                    logger.info(f"데이터가 정상적으로 업데이트되었습니다.")
                else:
                    sql = f"INSERT INTO news_{site} (datetime, title, text, link) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (row['datetime'] , row['title'],  row['text'],  row['link']))
                    logger.info(f"데이터가 정상적으로 추가되었습니다.")
    except Exception as e:
        logger.error(f"insert_or_update_news 함수에서 에러 발생: {e}")
        # 예외가 발생하면 트랜잭션을 취소
        cursor.execute("ROLLBACK")    
        logger.info(f"ROLLBACK 되었습니다.") 
    else:
        # 예외가 발생하지 않으면 트랜잭션을 커밋
        cursor.execute("COMMIT")    
        logger.info(f"COMMIT 되었습니다.") 
    finally:
        # 데이터베이스 연결 종료
        cursor.close()
        con.close()
        logger.info(f"데이터베이스 연결 종료합니다.") 
          
# import pandas as pd
# news_data = {'datetime': ['2022-01-01 12:00:00', '2022-01-02 13:00:00', '2022-01-03 14:00:00'], 
#              'title': ['News 1', 'News 2', 'News 3'], 
#              'category':['a','a','a'],
#              'text': ['Text 1', 'Text 2', 'Text 3'], 
#              'link': ['Link 1', 'Link 2', 'Link 3']}

# news_data = {'datetime': ['2022-01-01 12:00:00', '2022-01-02 13:00:00', '2022-01-03 14:00:00'], 
#              'title': ['News 1', 'News 2', 'News 3'], 
#              'text': ['Text 1', 'Text 2', 'Text 3'], 
#              'link': ['Link 1', 'Link 2', 'Link 3']}
# df = pd.DataFrame(news_data)
# insert_or_update_news('daum',df, 'localhost', 'dahy', '1234', 'news_db')