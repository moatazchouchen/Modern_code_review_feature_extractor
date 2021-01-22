# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from ComputeFeatures import ComputeStatistics
from CrawlMCRData import CrawlerLuncher
from Prepare_metadata import load_metadata_from_raw_data, preprocess_metadata, save_metadata


def crawl_project_mcr_raw_data(result_path, project_url, status_to_crawl=['abandoned', 'merged'],
                               query_details='&o=ALL_REVISIONS&o=ALL_FILES&o=ALL_COMMITS&o=MESSAGES&o=DETAILED_LABELS&o=DETAILED_ACCOUNTS&o=REVIEWER_UPDATES'):
    for current_status in status_to_crawl:
        print('crawling for status :', current_status)
        current_query = '/changes/?q=is:' + current_status + query_details
        current_crawler_launcher = CrawlerLuncher(project_url=project_url, base_dir=result_path + '/' + current_status,
                                                  crawl_config={'threads_number': 1, 'queries_per_thread': 500,
                                                                'span': 500,
                                                                'query': current_query,
                                                                'header': {
                                                                    'User-Agent': 'Bot_crawling_review_data_moataz_chouchen_Ets_montreal',
                                                                    'from': 'moataz.chouchen.1@ens.etsmtl.ca'}})
        current_crawler_launcher.run_crawling()


# please uncomment when necessary


# crawling data
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    PROJECT_PATH = 'C:/Users/AQ38570/Desktop/work/mcr_feature_extractor/libreoffice'
    PROJECT_URL = "https://review.libreoffice.org"
    ALL_PROJECT_STATUS = ['abondaned','merged']
    PROJECT_REPOS_PATH = 'C:/Users/AQ38570/Desktop/work/gitim-master/LibreOffice'
    # use this if you need to crawl data for current_project.
    # crawl_project_mcr_raw_data(result_path = PROJECT_PATH,project_url=PROJECT_URL,status_to_crawl=ALL_PROJECT_STATUS)
    data = load_metadata_from_raw_data(data_path=PROJECT_PATH,
                                       status=ALL_PROJECT_STATUS)
    preprocessed_metadata = preprocess_metadata(data)
    print('metadata is ready to use')
    # metadata can be saved for later usage/debugging issues
    save_metadata(preprocessed_metadata,
     'C:/Users/AQ38570/Desktop/work/mcr_feature_extractor/libreoffice_metadata.xlsx')
    project_features = ComputeStatistics(project_files_path=PROJECT_PATH, bots_accounts_ids=[], metadata=preprocessed_metadata, repos_path=PROJECT_REPOS_PATH, results_path='C:/Users/AQ38570/Desktop/work/mcr_feature_extractor')
    project_features.process_review_data()
    print('FIIIIIIIIIIIIIIIIIIIIIIN')