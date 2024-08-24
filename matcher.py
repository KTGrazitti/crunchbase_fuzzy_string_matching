import pandas as pd
import logging
from tqdm import tqdm
from preprocessor import URLPreprocessor
import os

class Matcher:
    def __init__(self, log_file='url_processing.log'):
        """
        Initialize the Matcher class.
        
        Args:
            log_file (str): Name of the log file.
        """
        # Set up logging
        logging.basicConfig(filename=log_file, level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize the URL preprocessor
        self.preprocessor = URLPreprocessor()

    def load_clean_and_rename_dataframe(self, file_path, id_column, url_column, rename_dict=None):
        """
        Load a CSV file into a DataFrame, clean it by removing null values, and rename columns if needed.
        
        Args:
            file_path (str): Path to the CSV file.
            id_column (str): Name of the ID column.
            url_column (str): Name of the URL column.
            rename_dict (dict, optional): Dictionary of columns to rename. Default is None.
        
        Returns:
            pd.DataFrame: Cleaned and renamed DataFrame.
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"The file {file_path} does not exist.")

            # Load the CSV file
            df = pd.read_csv(file_path)
            
            # Check if DataFrame is empty
            if df.empty:
                raise ValueError(f"The loaded DataFrame from {file_path} is empty.")
            
            # Log the initial shape of the DataFrame
            self.logger.info(f"Loaded DataFrame from {file_path}. Initial shape: {df.shape}")
            
            # Rename columns if rename_dict is provided
            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)
                self.logger.info(f"Renamed columns: {rename_dict}")
            
            # Check if required columns exist
            if id_column not in df.columns or url_column not in df.columns:
                raise KeyError(f"Required columns {id_column} or {url_column} not found in the DataFrame.")
            
            # Remove rows with null values in ID or URL columns
            initial_rows = len(df)
            df.dropna(subset=[id_column, url_column], inplace=True)
            rows_dropped = initial_rows - len(df)
            
            if rows_dropped > 0:
                self.logger.warning(f"Dropped {rows_dropped} rows with null values in {id_column} or {url_column}")
            
            # Log the shape after removing null values
            self.logger.info(f"Shape after removing null values: {df.shape}")
            
            return df

        except Exception as e:
            error_msg = f"Error in load_clean_and_rename_dataframe: {str(e)}"
            self.logger.error(error_msg)
            print(error_msg)  # Ensure the error is printed to the console
            return None

    def preprocess_urls(self, df, url_column, company_column):
        """
        Preprocess URLs in a DataFrame to extract company names.
        
        Args:
            df (pd.DataFrame): Input DataFrame.
            url_column (str): Name of the column containing URLs.
            company_column (str): Name of the new column to store extracted company names.
        
        Returns:
            pd.DataFrame: DataFrame with preprocessed URLs.
        """
        try:
            if df is None or df.empty:
                raise ValueError("Input DataFrame is None or empty.")

            # Ensure URL column is string type
            df[url_column] = df[url_column].astype(str)
            
            # Extract company names from URLs
            tqdm.pandas(desc=f"Extracting company names for {company_column}")
            df[company_column] = df[url_column].progress_apply(self.preprocessor.extract_domain_and_company)
            
            self.logger.info(f"Preprocessed URLs and extracted company names into '{company_column}' column")
            
            return df

        except Exception as e:
            error_msg = f"Error in preprocess_urls: {str(e)}"
            self.logger.error(error_msg)
            print(error_msg)  # Ensure the error is printed to the console
            return None

    def match_companies(self, df1, df2, company_col1, company_col2, id_col1, id_col2, url_col1, url_col2):
        """
        Match companies between two DataFrames.
        
        Args:
            df1 (pd.DataFrame): First DataFrame.
            df2 (pd.DataFrame): Second DataFrame.
            company_col1 (str): Name of the column containing company names in df1.
            company_col2 (str): Name of the column containing company names in df2.
            id_col1 (str): Name of the ID column in df1.
            id_col2 (str): Name of the ID column in df2.
            url_col1 (str): Name of the URL column in df1.
            url_col2 (str): Name of the URL column in df2.
        
        Returns:
            pd.DataFrame: DataFrame with matched companies.
        """
        try:
            if df1 is None or df2 is None or df1.empty or df2.empty:
                raise ValueError("One or both input DataFrames are None or empty.")

            # Perform exact matching
            results = self.preprocessor.process_companys(df1[company_col1], df2[company_col2], match_type='exact')
            
            # Add matching results to df1
            df1['exact_match'] = [match for _, match, _ in results]
            
            # Merge DataFrames based on exact matches
            result_df = pd.merge(df1, df2, left_on='exact_match', right_on=company_col2)
            
            # Select desired columns
            result_df = result_df[[id_col1, id_col2, url_col1, url_col2, company_col1, company_col2]]
            
            if result_df.empty:
                self.logger.warning("No matches found between the two DataFrames")
            else:
                self.logger.info(f"Matched {len(result_df)} companies between the two DataFrames")
            
            return result_df

        except Exception as e:
            error_msg = f"Error in match_companies: {str(e)}"
            self.logger.error(error_msg)
            print(error_msg)  # Ensure the error is printed to the console
            return None

    def save_results(self, matched_df, unmatched_df, matched_path, unmatched_path):
        """
        Save matched and unmatched results to CSV files.
        
        Args:
            matched_df (pd.DataFrame): DataFrame with matched companies.
            unmatched_df (pd.DataFrame): DataFrame with unmatched companies.
            matched_path (str): Path to save matched results.
            unmatched_path (str): Path to save unmatched results.
        """
        try:
            if matched_df is None or unmatched_df is None:
                raise ValueError("Matched or unmatched DataFrame is None")

            if matched_df.empty and unmatched_df.empty:
                raise ValueError("Both matched and unmatched DataFrames are empty")

            matched_df.to_csv(matched_path, index=False)
            unmatched_df.to_csv(unmatched_path, index=False)
            
            self.logger.info(f"Saved matched results to {matched_path}")
            self.logger.info(f"Saved unmatched results to {unmatched_path}")

        except Exception as e:
            error_msg = f"Error in save_results: {str(e)}"
            self.logger.error(error_msg)
            print(error_msg)

    def process(self, file_path1, file_path2, id_col1, id_col2, url_col1, url_col2, 
                company_col1, company_col2, matched_path, unmatched_path, 
                rename_dict1=None, rename_dict2=None, name=None):
        """
        Process two input files, match companies, and save results.
        
        Args:
            file_path1 (str): Path to the first input CSV file.
            file_path2 (str): Path to the second input CSV file.
            id_col1 (str): Name of the ID column in the first DataFrame.
            id_col2 (str): Name of the ID column in the second DataFrame.
            url_col1 (str): Name of the URL column in the first DataFrame.
            url_col2 (str): Name of the URL column in the second DataFrame.
            company_col1 (str): Name to use for the company column in the first DataFrame.
            company_col2 (str): Name to use for the company column in the second DataFrame.
            matched_path (str): Path to save matched results.
            unmatched_path (str): Path to save unmatched results.
            rename_dict1 (dict, optional): Dictionary of columns to rename in the first DataFrame. Default is None.
            rename_dict2 (dict, optional): Dictionary of columns to rename in the second DataFrame. Default is None.
            name(str): name of the process 
        """
        try:
            # Load, clean, and rename data
            df1 = self.load_clean_and_rename_dataframe(file_path1, id_col1, url_col1, rename_dict1)
            df2 = self.load_clean_and_rename_dataframe(file_path2, id_col2, url_col2, rename_dict2)
            
            if df1 is None or df2 is None:
                raise ValueError("One or both input DataFrames could not be loaded or processed")

            # Preprocess URLs and extract company names
            df1 = self.preprocess_urls(df1, url_col1, company_col1)
            df2 = self.preprocess_urls(df2, url_col2, company_col2)
            
            if df1 is None or df2 is None:
                raise ValueError("Error occurred during URL preprocessing")

            # Match companies
            matched_df = self.match_companies(df1, df2, company_col1, company_col2, id_col1, id_col2, url_col1, url_col2)
            
            if matched_df is None:
                raise ValueError("Error occurred during company matching")

            # Identify unmatched companies
            unmatched_df = df1[~df1[id_col1].isin(matched_df[id_col1])][[id_col1, url_col1]]
            
            # Save results
            self.save_results(matched_df, unmatched_df, matched_path, unmatched_path)

        except Exception as e:
            error_msg = f"Error in process: {str(e)}"
            self.logger.error(error_msg)
            print(error_msg)  # Ensure the error is printed to the console

# Example usage
if __name__ == "__main__":
    matcher = Matcher()
    matcher.process(
        file_path1='./input_data/VW_SF_CRM_MATCH.csv',
        file_path2='./input_data/VW_CB_MATCH.csv',
        id_col1='CRM_ID',
        id_col2='UUID',
        url_col1='COMPANY_WEBSITE',
        url_col2='HOMEPAGE_URL',
        company_col1='crm_company',
        company_col2='cb_company',
        matched_path="./match_tables/crm_cb_match.csv",
        unmatched_path="./umatch_tables/crm_cb_unmatched.csv",
        rename_dict1={"ID": "CRM_ID"},  # Example of renaming a column in the first DataFrame
        rename_dict2=None  # No renaming needed for the second DataFrame in this example
    )