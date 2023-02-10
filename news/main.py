import threading
from collect import CollectNews
import traceback
import sys

sys.path.append("/dahy/company_analysis")
from set_log import set_log



def main():
    logger=set_log()


    collectnews=CollectNews()

    try:
        logger.info(f"뉴스 검색을 시작합니다.")
        t0 = threading.Thread(target=collectnews.collect_naver, args=(0,))
        t1 = threading.Thread(target=collectnews.collect_naver, args=(1,))
        t2 = threading.Thread(target=collectnews.collect_naver, args=(2,))
        t3 = threading.Thread(target=collectnews.collect_naver, args=(3,))
        t4 = threading.Thread(target=collectnews.collect_naver, args=(4,))
        t5= threading.Thread(target=collectnews.collect_daum)
        t0.start()
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t0.join()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("main 함수에서 에러 발생: {}\nTraceback: {}".format(e, tb))


if __name__ == '__main__':
    main()
