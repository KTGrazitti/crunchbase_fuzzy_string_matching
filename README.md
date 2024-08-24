## Project Overview

This project is designed to match and process company names extracted from URLs across multiple datasets. It involves the extraction of company names from URLs, preprocessing them, and matching these company names across different datasets using various matching techniques (exact, fuzzy, etc.). The results are then saved in separate files for matched and unmatched entries.

### Project Structure

The project contains the following main scripts:

1. **preprocessor.py**: Contains the `URLPreprocessor` class, which handles the extraction and processing of company names from URLs.
2. **matcher.py**: Contains the `Matcher` class, which loads datasets, preprocesses the URLs, and matches companies across datasets.
3. **postprocess.py**: Contains the utility functions, which loads the matched URL tables, removes the duplicate URLs, and saves them into duplicate match tables.
3. **main.py**: The main script that coordinates the matching process between various company datasets.

### Dependencies

Ensure you have the following Python packages installed:
- `pandas`
- `tqdm`
- `tldextract`
- `polyleven`
- `logging`

You can install them using pip:
```bash
pip install pandas tqdm tldextract polyleven
```

### How to Run

1. **Prepare the Input Data**: Place your input CSV files in the `./input_data/` directory. The expected input files are:
   - `VW_SF_CRM_MATCH.csv`
   - `VW_CB_MATCH.csv`
   - `VW_SS_COMPANY_MATCH.csv`
   - `VW_BD_COMPANY_MATCH.csv`
   - `VW_BD_PEOPLE_MATCH.csv`

2. **Run the Main Script**:
   - To start the matching process, run the `main.py` script:
     ```bash
     python main.py
     ```

3. **Check the Results**:
   - Matched company names will be saved in the `./match_tables/` directory.
   - Unmatched company names will be saved in the `./unmatch_tables/` directory.

### Results

The results will be stored in CSV format in the following locations:

- **Matched Results**: Stored in the `./match_tables/` directory. Files include:
  - `crm_cb_matched.csv`: Distinct Matches between CRM and Crunchbase companies.
  - `crm_ss_matched.csv`: Distinct Matches between CRM and Sourcescrub companies.
  - `cb_ss_matched.csv`: Distinct Matches between Crunchbase and Sourcescrub companies.
  - `crm_bd_comp_matched.csv`: Distinct Matches between CRM and Brightdata companies.
  - `crm_bd_linkedin_comp_matched.csv`: Distinct Matches between CRM and Brightdata LinkedIn companies.
  - `crm_bd_linkedin_people_matched.csv`: Distinct Matches between CRM and Brightdata LinkedIn Profile URLs.


- **Duplicate Match Results**: Stored in the `./match_tables/` directory. Files include:
  - `crm_cb_duplicate_matched.csv`: Matched duplicate domains for CRM and Crunchbase URLs
  - `crm_ss_duplicate_matched.csv`: Matched duplicate domains for CRM and Sourcescrub URLs
  - `cb_ss_duplicate_matched.csv`: Matches between Crunchbase and Sourcescrub companies.
  - `crm_bd_comp_duplicate_matched.csv`: Matched duplicate domains for CRM and Brightdata company website URLs
  - `crm_bd_linkedin_comp_duplicate_matched.csv`: Matched duplicate domains for CRM URLs and Brightdata LinkedIn company URLs (secondary match)
  - `crm_bd_linkedin_people_duplicate_matched.csv`: Matched duplicate CRM and Brightdata LinkedIn profile URLs

- **Unmatched Results**: Stored in the `./unmatch_tables/` directory. Files include:
  - `crm_cb_unmatched.csv`: Unmatched CRM companies when compared with Crunchbase companies.
  - `crm_ss_unmatched.csv`: Unmatched CRM companies when compared with Sourcescrub companies.
  - `cb_ss_unmatched.csv`: Unmatched Crunchbase companies when compared with Sourcescrub companies.
  - `crm_bd_comp_unmatched.csv`: Unmatched CRM companies when compared with Brightdata companies.
  - `crm_bd_linkedin_comp_unmatched.csv`: Unmatched CRM companies when compared with Brightdata LinkedIn companies.
  - `crm_bd_linkedin_people_unmatched.csv`: Unmatched CRM People Linkedin URLs when compared with Brightdata LinkedIn People URLs.

### Customizing the Matching Process

You can customize the matching process by adjusting the parameters in the `process` method calls in `main.py`. For example, you can change the matching type (exact or fuzzy), or modify the column names as per your dataset's structure.

### Logs

The script logs its operations to a file named `url_matching_main.log`. This log file contains detailed information about the loading of datasets, preprocessing steps, and matching outcomes.