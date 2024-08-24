import re
import tldextract
from typing import Literal, List, Tuple
from urllib.parse import unquote

import pandas as pd
from polyleven import levenshtein
from tqdm import tqdm

class URLPreprocessor:
    def __init__(self):

        # Initialize common terms set
        self.common_terms, self.country_codes = self._initialize_common_terms()

    def _initialize_common_terms(self) -> set:
        """
        Initialize a set of common terms and country codes to exclude from company names.
        
        Returns:
            set: A set of common terms and country_codes
        """
        common_terms = {
            # Generic TLDs
            'com', 'org', 'net', 'info', 'biz', 'edu', 'gov', 'mil', 'int',
            'ac', 'co', 'io', 'ai', 'app', 'dev', 'tech', 'online', 'store', 'shop',
            'blog', 'site', 'cloud', 'design', 'agency', 'marketing', 'digital',

            # Country Code TLDs
            'us', 'uk', 'ca', 'au', 'de', 'fr', 'jp', 'cn', 'in', 'ru', 'br', 'it',
            'nl', 'es', 'se', 'no', 'fi', 'dk', 'ch', 'at', 'be', 'nz', 'sg', 'ae',
            'kr', 'za', 'mx', 'ar', 'cl', 'pl', 'cz', 'gr', 'hu', 'pt', 'ro', 'th',
            'tr', 'ua', 'vn', 'ph', 'my', 'id', 'tw', 'hk', 'me', 'eu',

            # Common URL words and abbreviations
            'about', 'info', 'contact', 'support', 'help', 'faq', 'news', 'blog',
            'corp', 'corporate', 'company', 'inc', 'incorporated', 'llc', 'ltd',
            'limited', 'group', 'intl', 'international', 'global', 'worldwide',
            'local', 'official', 'home', 'main', 'index', 'web', 'site', 'portal',
            'login', 'signup', 'register', 'account', 'user', 'customer', 'client',
            'partner', 'vendor', 'supplier', 'en', 'eng', 'english', 'fr', 'fra', 
            'french', 'de', 'deu', 'german', 'es', 'esp', 'spanish', 'it', 'ita', 
            'italian', 'pt', 'por', 'portuguese', 'ru', 'rus', 'russian', 'cn', 
            'chi', 'chinese', 'jp', 'jpn', 'japanese',

            # E-commerce related
            'shop', 'store', 'buy', 'sell', 'sale', 'discount', 'deal', 'offer',
            'product', 'item', 'catalog', 'category', 'cart', 'checkout', 'payment',
            'order', 'shipping', 'delivery',

            # Business and corporate terms
            'careers', 'jobs', 'hr', 'human-resources', 'recruitment', 'investor',
            'investors', 'shareholders', 'press', 'media', 'pr', 'public-relations',
            'legal', 'privacy', 'terms', 'conditions', 'policy', 'policies',
            'services', 'solutions', 'products', 'projects', 'portfolio', 'business',

            # Technology-related
            'app', 'apps', 'api', 'dev', 'developer', 'webmaster', 'admin', 'sys',
            'system', 'network', 'host', 'domain', 'email', 'mail', 'webmail',
            'cloud', 'server', 'database', 'data', 'analytics', 'stats', 'metrics',

            # Social media and community
            'social', 'community', 'forum', 'chat', 'discuss', 'connect', 'follow',
            'like', 'share', 'tweet', 'post', 'profile', 'user', 'member',

            # Miscellaneous
            'page', 'pages', 'site', 'sites', 'web', 'internet', 'online', 'digital',
            'virtual', 'mobile', 'desktop', 'platform', 'service', 'tool', 'resource',
            'guide', 'tutorial', 'learn', 'education', 'training', 'course', 'program'
        }
        
        # Comprehensive list of country code TLDs
        country_codes = {
            'ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'ao', 'aq', 'ar', 'as', 'at', 'au', 'aw', 'ax', 'az',
            'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bl', 'bm', 'bn', 'bo', 'bq', 'br', 'bs', 'bt',
            'bv', 'bw', 'by', 'bz', 'ca', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck', 'cl', 'cm', 'cn', 'co', 'cr',
            'cu', 'cv', 'cw', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm', 'do', 'dz', 'ec', 'ee', 'eg', 'eh', 'er',
            'es', 'et', 'eu', 'fi', 'fj', 'fk', 'fm', 'fo', 'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi',
            'gl', 'gm', 'gn', 'gp', 'gq', 'gr', 'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu',
            'id', 'ie', 'il', 'im', 'in', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh',
            'ki', 'km', 'kn', 'kp', 'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu',
            'lv', 'ly', 'ma', 'mc', 'md', 'me', 'mf', 'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mo', 'mp', 'mq', 'mr',
            'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'nc', 'ne', 'nf', 'ng', 'ni', 'nl', 'no', 'np',
            'nr', 'nu', 'nz', 'om', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl', 'pm', 'pn', 'pr', 'ps', 'pt', 'pw',
            'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb', 'sc', 'sd', 'se', 'sg', 'sh', 'si', 'sk', 'sl',
            'sm', 'sn', 'so', 'sr', 'ss', 'st', 'sv', 'sx', 'sy', 'sz', 'tc', 'td', 'tf', 'tg', 'th', 'tj', 'tk',
            'tl', 'tm', 'tn', 'to', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'uy', 'uz', 'va', 'vc',
            've', 'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'ye', 'yt', 'za', 'zm', 'zw'
        }
        return common_terms, country_codes

    def extract_domain_and_company(self, url: str) -> str:
        """
        Extract domain and company name from a given URL.
        
        Args:
            url (str): The input URL.
        
        Returns:
            str: The extracted company name.
        """
        # Handle unusual 'http//' cases
        url = re.sub(r'^https?://http//', 'http://', url)
        
        # Use tldextract to properly parse the URL
        extracted = tldextract.extract(url)

        # If URL is linkedin, facebook or zaubacorp url then change preprocessing
        if extracted.domain in ['facebook', 'zaubacorp', 'linkedin']:
            linkedin_processed = self.preprocess_linkedin_url(url)
            if linkedin_processed:
                return linkedin_processed
        
        # Combine domain and suffix
        full_domain = f"{extracted.domain}.{extracted.suffix}"
        
        # Define common country code TLDs and known common suffixes
        country_codes = self.country_codes
        common_suffixes = {'business', 'webflow', 'site', 'com'}
        
        # Handle cases where the domain is a country code or a common suffix
        if extracted.subdomain and (extracted.domain in country_codes or extracted.domain in common_suffixes):
            company_name = f"{extracted.subdomain}.{full_domain}"
        else:
            # Standard case
            company_name = full_domain
        
        # Remove common prefixes from the domain
        common_prefixes = ['www', 'web', 'corp', 'corporate', 'about', 'info', 'shop', 'company']
        domain_parts = company_name.split('.')
        cleaned_domain_parts = [part for part in domain_parts if part.lower() not in common_prefixes]
        
        # Join the cleaned domain parts
        company_name = '.'.join(cleaned_domain_parts)
        
        return company_name.lower()

    def preprocess_linkedin_url(self, url: str) -> str:
        """
        Preprocess LinkedIn URL to extract company name.
        
        Args:
            url (str): The input LinkedIn URL.
        
        Returns:
            str: The extracted company name.
        """
        # Remove query parameters and 'about' section
        url = re.sub(r'(\?.*)|(\/about.*)', '', url)
        
        # Decode any percent-encoded characters
        url = unquote(url)
        
        # Determine if the URL is a profile or company URL and extract accordingly
        if '/company/' in url:
            entity_name = url.rstrip('/').split('/')[-1]
        elif '/in/' in url:
            entity_name = url.rstrip('/').split('/')[-1]
        else:
            entity_name = url
        
        # Normalize by lowercasing and removing special characters (except hyphens)
        entity_name = re.sub(r'[^\w\-]', '', entity_name.lower())
        
        return entity_name

    def extract_company_name(self, url: str) -> str:
        """
        Extract company name from a given URL.
        
        Args:
            url (str): The input URL.
        
        Returns:
            str: The extracted company name.
        """

        # Remove protocol, www, and handle the unusual http// case
        url = re.sub(r'^(https?://)?(http//)?((www|web)\.)?', '', url)
        
        # Split the remaining URL by dots and slashes
        parts = re.split(r'[./]', url)
        
        # Filter out common terms and empty strings
        filtered_parts = [part for part in parts if part and part.lower() not in self.common_terms]
        
        # If filtered_parts is empty, use the original parts without filtering
        if not filtered_parts:
            filtered_parts = [part for part in parts if part]
            
        # Join remaining parts
        company_name = '-'.join(filtered_parts)
        
        # Remove common prefixes and suffixes
        company_name = re.sub(r'^(corp-|corporate-|about-|info-|en-|shop-)', '', company_name)
        company_name = re.sub(r'-(corp|corporate|about|info|en|shop)$', '', company_name)
        
        # If company_name is still empty, use the second level domain
        if not company_name and len(parts) >= 2:
            company_name = parts[-2]
        
        return company_name

    def find_best_match_with_distance_poly(self, cd_company: str, crm_companys: List[str]) -> Tuple[str, float]:
        """
        Find the best match for a company name using Levenshtein distance.
        
        Args:
            cd_company (str): The company name to match.
            crm_companys (List[str]): List of company names to match against.
        
        Returns:
            Tuple[str, float]: The best matching company name and its Levenshtein distance.
        """
        best_match = None
        min_distance = float('inf')
        for crm_company in crm_companys:
            dist = levenshtein(cd_company, crm_company)
            if dist < min_distance:
                min_distance = dist
                best_match = crm_company
        return best_match, min_distance

    def process_companys(self, crm_companys: List[str], cd_companys: List[str], match_type: Literal["exact", "fuzzy", None] = None) -> List[Tuple[str, str, float]]:
        """
        Process company names to find matches.
        
        Args:
            crm_companys (List[str]): List of CRM company names.
            cd_companys (List[str]): List of CD company names.
            match_type (Literal["exact", "fuzzy", None]): Type of matching to perform.
        
        Returns:
            List[Tuple[str, str, float]]: List of tuples containing (crm_company, matched_cd_company, distance).
        """
        results = []
        if match_type == "exact":
            set1 = set(cd_companys)
            results = [(item, item, 0) if item in set1 else (item, None, float('inf')) for item in tqdm(crm_companys, desc="Exact Matching")]
        else:
            for crm_company in tqdm(crm_companys, desc="Fuzzy Matching"):
                best_match, distance = self.find_best_match_with_distance_poly(crm_company, cd_companys)
                results.append((crm_company, best_match, distance))
        
        return results

# Example usage
if __name__ == "__main__":
    preprocessor = URLPreprocessor()
    
    # Example URLs
    urls = [
        "https://www.example.com",
        "http://linkedin.com/company/microsoft",
        "https://apple.com/about",
        "http//unusual-url-format.org"
    ]
    
    for url in urls:
        company_name = preprocessor.extract_company_name(url)
        print(f"URL: {url}")
        print(f"Extracted Company Name: {company_name}")
        print()

    # Example company matching
    crm_companys = ["Apple Inc.", "Microsoft Corporation", "Google LLC"]
    cd_companys = ["apple", "microsoft", "alphabet"]
    
    results = preprocessor.process_companys(crm_companys, cd_companys, match_type="fuzzy")
    for crm_company, matched_cd_company, distance in results:
        print(f"CRM Company: {crm_company}")
        print(f"Matched CD Company: {matched_cd_company}")
        print(f"Distance: {distance}")
        print()