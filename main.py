import pandas as pd
import logging
from matcher import Matcher
import os

from postprocess import process_all_matched_outputs
from postprocess import get_bd_linkedin_company_urls

# Set up logging
logging.basicConfig(filename='url_matching_main.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    try:
        matcher = Matcher()

        # List of matching operations
        matching_operations = [
        {
                "name": "CRM to Crunchbase Company URL Matching",
                "file_path1": './input_data/VW_SF_CRM_MATCH.csv',
                "file_path2": './input_data/VW_CB_MATCH.csv',
                "id_col1": 'CRM_ID',
                "id_col2": 'UUID',
                "url_col1": 'COMPANY_WEBSITE',
                "url_col2": 'HOMEPAGE_URL',
                "company_col1": 'crm_company',
                "company_col2": 'cb_company',
                "matched_path": "./match_tables/crm_cb_matched.csv",
                "unmatched_path": "./unmatch_tables/crm_cb_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": None
            },
            {
                "name": "CRM to Sourcescrub Company URL Matching",
                "file_path1": './input_data/VW_SF_CRM_MATCH.csv',
                "file_path2": './input_data/VW_SS_COMPANY_MATCH.csv',
                "id_col1": 'CRM_ID',
                "id_col2": 'ID',
                "url_col1": 'COMPANY_WEBSITE',
                "url_col2": 'WEBSITE',
                "company_col1": 'crm_company',
                "company_col2": 'ss_company',
                "matched_path": "./match_tables/crm_ss_matched.csv",
                "unmatched_path": "./unmatch_tables/crm_ss_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": None
            },
            {
                "name": "Crunchbase to Sourcescrub Company URL Matching",
                "file_path1": './input_data/VW_CB_MATCH.csv',
                "file_path2": './input_data/VW_SS_COMPANY_MATCH.csv',
                "id_col1": 'UUID',
                "id_col2": 'ID',
                "url_col1": 'HOMEPAGE_URL',
                "url_col2": 'WEBSITE',
                "company_col1": 'cb_company',
                "company_col2": 'ss_company',
                "matched_path": "./match_tables/cb_ss_matched.csv",
                "unmatched_path": "./unmatch_tables/cb_ss_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": None
            },
            {
                "name": "CRM to Brightdata Company Website URL Matching",
                "file_path1": './input_data/VW_SF_CRM_MATCH.csv',
                "file_path2": './input_data/VW_BD_COMPANY_MATCH.csv',
                "id_col1": 'CRM_ID',
                "id_col2": 'ID',
                "url_col1": 'COMPANY_WEBSITE',
                "url_col2": 'WEBSITE',
                "company_col1": 'crm_company',
                "company_col2": 'bd_company',
                "matched_path": "./match_tables/crm_bd_comp_matched.csv",
                "unmatched_path": "./unmatch_tables/crm_bd_comp_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": {"COMPANY_WEBSITE": "WEBSITE"}
            },
            {
                "name": "CRM to Brightdata Linkedin Company URL Matching",
                "file_path1": './input_data/linkedin_crm_comp.csv',
                "file_path2": './input_data/linkedin_bd_comp.csv',
                "id_col1": 'CRM_ID',
                "id_col2": 'ID',
                "url_col1": 'LINKEDIN_URL_COMPANY',
                "url_col2": 'LINKEDIN_URL',
                "company_col1": 'crm_company',
                "company_col2": 'bd_company',
                "matched_path": "./match_tables/crm_bd_linkedin_comp_matched.csv",
                "unmatched_path": "./unmatch_tables/crm_bd_comp_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": None
            },
            {
                "name": "CRM to Brightdata Linkedin People URL Matching",
                "file_path1": './input_data/VW_SF_CRM_MATCH.csv',
                "file_path2": './input_data/VW_BD_PEOPLE_MATCH.csv',
                "id_col1": 'CRM_ID',
                "id_col2": 'ID',
                "url_col1": 'LINKEDIN_URL_PERSON',
                "url_col2": 'LINKEDIN_URL_PERSON_BD',
                "company_col1": 'crm_company',
                "company_col2": 'bd_company',
                "matched_path": "./match_tables/crm_bd_people_matched.csv",
                "unmatched_path": "./unmatch_tables/crm_bd_people_unmatched.csv",
                "rename_dict1": {"ID": "CRM_ID"},
                "rename_dict2": {"LINKEDIN_URL_PERSON": "LINKEDIN_URL_PERSON_BD"}
            }
        ]

        for operation in matching_operations:
            logger.info(f"Starting {operation['name']}")
            try:
                if operation['name'] == 'CRM to Brightdata Linkedin People URL Matching':
                    # Generate input of brightdata linkedin company Url Match
                    logger.info("Generating input for Brightdata LinkedIn company URL matching")
                    get_bd_linkedin_company_urls()
                matcher.process(**operation)
                logger.info(f"Completed {operation['name']}")
            except Exception as e:
                logger.error(f"Error in {operation['name']}: {str(e)}")
                print(f"Error in {operation['name']}: {str(e)}")

        # duplicacy checking and generating duplicacy tables for all matched outputs
        process_all_matched_outputs(matching_operations)

        logger.info("URL matching process completed successfully")

    except Exception as e:
        logger.error(f"Unexpected error in main function: {str(e)}")
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()