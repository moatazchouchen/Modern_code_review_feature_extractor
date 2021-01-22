from pygerrit2 import GerritRestAPI, Anonymous
import json
from concurrent import futures
import time
import os
from fake_useragent import UserAgent


class MultithreadingReviewCrawler:
    def __init__(self, rest_api, base_url, base_dir, max_thread_number, queries_per_thread, start_position, span,
                 header):

        self.header = header
        self.rest_api = rest_api
        self.base_dir = base_dir
        self.max_thread_number = max_thread_number
        self.queries_per_thread = queries_per_thread
        self.span = span
        self.finished = False
        self.final_position = -1
        self.base_url = base_url
        self.start_position = start_position
        self.reviews_problems = []

    def crawl_job(self, tid, start_position):
        end_position = start_position + self.queries_per_thread
        print(str(tid) + ":starting from " + str(start_position) + " to " + str(end_position))
        for current_pos in range(start_position, start_position + self.queries_per_thread, self.span):
            # do the query
            collected_changes = self.run_attempts(10, current_pos)
            if collected_changes == None:
                if tid == self.max_thread_number - 1:
                    # TODO : recheck final condition
                    self.finished = True
                    self.final_position = current_pos
                    print("Crawling is done")
                    return
                print("could not solve for : " + str(current_pos) + ' - ' + str(current_pos + self.span))
                self.reviews_problems.append([current_pos, current_pos + self.span])
                continue
            self.save_changes(collected_changes, current_pos)

        print(str(tid) + ": from " + str(start_position) + " to " + str(end_position) + ' finished')

    def run_attempts(self, attemps_number, current_pos):
        timeout = 500
        for attempt_count in range(attemps_number):
            try:
                collected_changes = self.rest_api.get(self.base_url + '&n=' + str(self.span) + '&S=' + str(current_pos),
                                                      timeout=timeout, headers=self.header)
            except:
                print('problem occured')
                timeout += 500
                continue
            if len(collected_changes) != 0:
                return collected_changes
            else:
                print('another attempt!')
        return None

    def save_changes(self, collected_changes, current_pos):
        with open(os.path.join(self.base_dir, str(current_pos) + '_' + str(current_pos + self.span) + '.json'),
                  'w') as query_file:
            json.dump(collected_changes, query_file, indent=4, sort_keys=True)

    def run(self):
        current_start_pos = self.start_position
        while not (self.finished):
            pool = futures.ThreadPoolExecutor(max_workers=self.max_thread_number)
            waiting_for = []
            for tid in range(self.max_thread_number):
                waiting_for.append(pool.submit(self.crawl_job, tid, current_start_pos))
                current_start_pos += self.queries_per_thread
            for completed in futures.as_completed(waiting_for):
                continue
            time.sleep(15)
        return self.final_position, self.reviews_problems


class CrawlerLuncher:
    def __init__(self, project_url, base_dir, crawl_config=None):
        self.project_url = project_url

        self.base_dir = base_dir
        self.project_exsist = False
        ua = UserAgent()
        header = {'User-Agent': str(ua.chrome)}
        self.default_config = {'current_position': 0, 'queries_per_thread': 500, 'span': 100, 'threads_number': 4,
                               'query': '/changes/?o=ALL_REVISIONS&o=ALL_FILES&o=ALL_COMMITS&o=MESSAGES&o=DETAILED_ACCOUNTS',
                               'header': header}
        self.config = self.default_config
        os.makedirs(os.path.join(base_dir, 'data'), exist_ok=True)
        if 'config.json' in os.listdir(self.base_dir):
            self.project_exsist = True
            self.config = json.load(open(os.path.join(base_dir, 'config.json')))
            current_index = self.recover()
            self.config['current_position'] = current_index
        if crawl_config != None:
            for config in crawl_config:
                self.config[config] = crawl_config[config]
        with open(os.path.join(self.base_dir, 'config.json'), 'w') as config_file:
            json.dump(self.config, config_file)
        auth = Anonymous()
        self.rest = GerritRestAPI(url=self.project_url, auth=auth)
        self.crawler = MultithreadingReviewCrawler(rest_api=self.rest, base_dir=self.base_dir + '/data',
                                                   base_url=self.config['query'],
                                                   max_thread_number=self.config['threads_number'],
                                                   queries_per_thread=self.config['queries_per_thread'],
                                                   start_position=self.config['current_position'],
                                                   span=self.config['span'], header=self.config['header'])

    def run_crawling(self):
        last_position, not_querried = self.crawler.run()
        self.config['unquerried reviews'] = [{'start': x[0], 'end : ': x[1]} for x in not_querried]
        self.config['current_position'] = last_position
        with open(os.path.join(self.base_dir, 'config.json'), 'w') as config_file:
            json.dump(self.config, config_file)

    def recover(self):
        current_pos = 0
        for file_name in os.listdir(os.path.join(self.base_dir, 'data')):
            if '.json' in file_name:
                second_index = int(file_name.split('_')[1].split('.')[0])
                if current_pos < second_index:
                    current_pos = second_index
        return current_pos


#lunch = CrawlerLuncher(project_url="https://review.opendev.org", base_dir='./opendev/abandoned',
 #                      crawl_config={'threads_number': 1, 'queries_per_thread': 500, 'span': 500,
  #                                   'query': '/changes/?q=is:abandoned&o=ALL_REVISIONS&o=ALL_FILES&o=ALL_COMMITS&o=MESSAGES&o=DETAILED_LABELS&o=DETAILED_ACCOUNTS&o=REVIEWER_UPDATES',
   #                                  'header': {'User-Agent': 'Bot_crawling_review_data_moataz_chouchen_Ets_montreal',
    #                                            'from': 'moataz.chouchen.1@ens.etsmtl.ca'}})
#lunch.run_crawling()
