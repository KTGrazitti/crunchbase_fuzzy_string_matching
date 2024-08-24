import pandas as pd
from matcher import Matcher
from postprocess import get_bd_linkedin_company_urls, process_all_matched_outputs

# Initialize the matcher object
matcher = Matcher()

# Test Case 1: Non-existent file
print("Test Case 1: Non-existent file")
result = matcher.load_clean_and_rename_dataframe('dummy.csv', 'id', 'url')
print("Output:", result)
print()

# Test Case 2: Empty DataFrame
print("Test Case 2: Empty DataFrame")
empty_df = pd.DataFrame()
result = matcher.preprocess_urls(empty_df, 'url', 'company')
print("Output:", result)
print()

# Test Case 3: DataFrame with null values
print("Test Case 3: DataFrame with null values")
df_with_nulls = pd.DataFrame({
    'id': [1, 2, None],
    'url': ['http://example.com', None, 'http://test.com']
})
result = matcher.load_clean_and_rename_dataframe(df_with_nulls, 'id', 'url')
print("Output:\n", result)
print()

# Test Case 4: DataFrame with missing columns
print("Test Case 4: DataFrame with missing columns")
df_missing_columns = pd.DataFrame({
    'id': [1, 2],
    'name': ['Example Inc.', 'Test Corp.']
})
try:
    result = matcher.load_clean_and_rename_dataframe(df_missing_columns, 'id', 'url')
except KeyError as e:
    print("Output: KeyError occurred:", e)
print()

# Test Case 5: Empty DataFrame during preprocessing
print("Test Case 5: Empty DataFrame during preprocessing")
try:
    result = matcher.preprocess_urls(empty_df, 'url', 'company')
except ValueError as e:
    print("Output: ValueError occurred:", e)
print()

# Test Case 6: Matching with empty DataFrames
print("Test Case 6: Matching with empty DataFrames")
try:
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    result = matcher.match_companies(df1, df2, 'company1', 'company2', 'id1', 'id2', 'url1', 'url2')
except ValueError as e:
    print("Output: ValueError occurred:", e)
print()

# Test Case 7: Process non-existent matched outputs
print("Test Case 7: Process non-existent matched outputs")
try:
    operations = [{
        "matched_path": 'dummy_matched.csv',
        "unmatched_path": 'dummy_unmatched.csv'
    }]
    process_all_matched_outputs(operations)
except FileNotFoundError as e:
    print("Output: FileNotFoundError occurred:", e)
except Exception as e:
    print("Output: Exception occurred:", e)
print()

# Test Case 8: LinkedIn URL processing with incorrect data
print("Test Case 8: LinkedIn URL processing with incorrect data")
try:
    get_bd_linkedin_company_urls()
except Exception as e:
    print("Output: Exception occurred:", e)
print()

# Test Case 9: Test domain extraction and matching
print("Test Case 9: Test domain extraction and matching")
df1 = pd.DataFrame({
    'id1': [1, 2],
    'url1': ['http://example.com', 'http://test.com'],
    'company1': ['example inc', 'test corp']
})
df2 = pd.DataFrame({
    'id2': [1, 2],
    'url2': ['http://example.com', 'http://test.com'],
    'company2': ['example inc', 'test corp']
})
result = matcher.match_companies(df1, df2, 'company1', 'company2', 'id1', 'id2', 'url1', 'url2')
print("Output:\n", result)
print()

# Test Case 10: Test process with correct files but empty content
print("Test Case 10: Test process with correct files but empty content")
df_empty = pd.DataFrame(columns=['id', 'url', 'company'])
df_empty.to_csv('dummy_matched.csv', index=False)
try:
    process_all_matched_outputs([{
        "matched_path": 'dummy_matched.csv',
        "unmatched_path": 'dummy_unmatched.csv'
    }])
except Exception as e:
    print("Output: Exception occurred:", e)
print()