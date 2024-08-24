import pandas as pd
import logging
import os
from typing import List, Dict

logging.basicConfig(filename='url_matching_main.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_bd_linkedin_company_urls(): 
    """
    Function to create an input of brightdata company linkedin urls \
        which not matched with company website to process them separately
    """
    try:
        # loading matched company website urls
        result_df = pd.read_csv("./match_tables/crm_bd_comp_matched.csv")

        # load the CRM URLs
        crm_df = pd.read_csv('./input_data/VW_SF_CRM_MATCH.csv')

        # load BD urls
        bd_df = pd.read_csv('./input_data/VW_BD_COMPANY_MATCH.csv')

        # Check if DataFrames are empty
        if result_df.empty or crm_df.empty or bd_df.empty:
            raise ValueError("One or more input DataFrames are empty")

        # Generate input for linkedin company url matching 
        unmatched_crm = crm_df[~crm_df['ID'].isin(result_df['CRM_ID'].to_list())]
        unmatched_bd = bd_df[~bd_df['ID'].isin(result_df['ID'].to_list())]

        # Check if unmatched DataFrames are empty
        if unmatched_crm.empty or unmatched_bd.empty:
            logger.warning("No unmatched records found for LinkedIn company URL matching")
        else:
            unmatched_crm.to_csv("./input_data/linkedin_crm_comp.csv", index=False)
            unmatched_bd.to_csv("./input_data/linkedin_bd_comp.csv", index=False)
            logger.info("Generated input files for LinkedIn company URL matching")

    except FileNotFoundError as e:
        error_msg = f"File not found: {str(e)}"
        logger.error(error_msg)
        print(error_msg)  # Ensure the error is printed to the console
    except pd.errors.EmptyDataError as e:
        error_msg = f"Empty CSV file: {str(e)}"
        logger.error(error_msg)
        print(error_msg)  # Ensure the error is printed to the console
    except Exception as e:
        error_msg = f"Error in get_bd_linkedin_company_urls: {str(e)}"
        logger.error(error_msg)
        print(error_msg)  # Ensure the error is printed to the console

def process_matched_output(operation: Dict[str, str]):
    """
    Process the matched output file, detect duplicates in the last column,
    remove all instances of duplicates (including originals), group duplicates together,
    and save to separate files.

    Args:
    operation (dict): A dictionary containing the operation details.

    Returns:
    None
    """
    try:
        # Extract the relevant paths and column names
        matched_path = operation['matched_path']
        file_name = os.path.basename(matched_path)
        dir_name = os.path.dirname(matched_path)
        
        # Construct the path for the duplicate file
        duplicate_file = os.path.join(dir_name, file_name.replace('matched', 'duplicate_matched'))
        
        # Read the CSV file
        df = pd.read_csv(matched_path)
        logging.info(f"Read {matched_path} successfully. Shape: {df.shape}")
        
        # Identify the last column
        last_column = df.columns[-1]
        logging.info(f"Checking for duplicates in column: {last_column}")
        
        # Find duplicates and non-duplicates
        duplicates = df[df.duplicated(subset=[last_column], keep=False)]
        non_duplicates = df.drop_duplicates(subset=[last_column], keep=False)
        
        # Sort duplicates by the last column to group them together
        duplicates_sorted = duplicates.sort_values(by=[last_column])
        
        # Log the results
        logging.info(f"Found {len(duplicates)} rows with duplicates based on {last_column}")
        logging.info(f"Remaining non-duplicate rows: {len(non_duplicates)}")
        
        # Save sorted duplicates to a new file
        duplicates_sorted.to_csv(duplicate_file, index=False)
        logging.info(f"Saved rows with duplicates (grouped together) to {duplicate_file}")
        
        # Save non-duplicates back to the original file
        non_duplicates.to_csv(matched_path, index=False)
        logging.info(f"Saved non-duplicate rows back to {matched_path}")
        
    except Exception as e:
        error_msg = f"An error occurred while processing {matched_path}: {str(e)}"
        logger.error(error_msg)
        print(error_msg)  # Ensure the error is printed to the console
        raise

def process_all_matched_outputs(operations: List[Dict[str, str]]):
    """
    Process all matched outputs for the given operations.

    Args:
    operations (list): A list of dictionaries, each containing operation details.

    Returns:
    None
    """
    for operation in operations:
        try:
            logging.info(f"Processing matched output for operation: {operation['matched_path']}")
            process_matched_output(operation)
        except Exception as e:
            error_msg = f"Failed to process operation: {operation['matched_path']}. Error: {str(e)}"
            logging.error(error_msg)
            print(error_msg)  # Ensure the error is printed to the console
            continue

# Usage example:
# process_all_matched_outputs(matching_operations)