"""
Airtable Integration Module

Things it does
- Fetch records from Airtable tables
- Convert between Airtable and DataFrame formats
- Standardize field names for consistent data structure
- Upload deduplication results to Airtable
- Handle batch processing and error management
- Provide table management utilities
"""

import os
import requests
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import json
import pandas as pd
import re

# Configure logging for debugging and monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables for Airtable configuration
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'All Providers')

# Configuration for linked record fields
# Add field names here that should be treated as linked records
LINKED_RECORD_FIELDS = {
    'Events', 
    # Add more field names as needed
}

# Construct the base URL for Airtable API calls
AIRTABLE_API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

# Standard headers required for all Airtable API requests
AIRTABLE_HEADERS = {
    'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    'Content-Type': 'application/json'
}

def validate_airtable_config() -> bool:
    """
    Validate that all required Airtable environment variables are properly configured.
    
    This function checks if the essential environment variables (API key and base ID)
    are set before attempting any Airtable operations. It's called by most functions
    to ensure the integration is properly configured.
    
    Returns:
        bool: True if all required variables are set, False otherwise
        
   """
    if not AIRTABLE_API_KEY:
        logger.error("AIRTABLE_API_KEY not set - please configure your Airtable API key")
        return False
    if not AIRTABLE_BASE_ID:
        logger.error("AIRTABLE_BASE_ID not set - please configure your Airtable base ID")
        return False
    return True

def fetch_airtable_records(table_name: str = None) -> List[Dict]:
    """
    Fetch all records from a specified Airtable table.
    
    This function retrieves all records from the specified Airtable table and returns
    them in Airtable's native format (with 'id' and 'fields' structure). If no table
    name is provided, it uses the default table from environment variables.
    
    Args:
        table_name (str, optional): Name of the Airtable table to fetch from.
                                   Defaults to AIRTABLE_TABLE_NAME environment variable.
    
    Returns:
        List[Dict]: List of Airtable records, each containing 'id' and 'fields' keys.
                   Returns empty list if configuration is invalid or API call fails.
    
"""
    if not validate_airtable_config():
        return []
    
    table = table_name or AIRTABLE_TABLE_NAME
    url = f"{AIRTABLE_API_URL}/{table}"
    
    try:
        logger.info(f"Fetching records from Airtable table: {table}")
        
        all_records = []
        offset = None
        
        while True:
            # Prepare parameters for this request
            params = {'pageSize': 100}  # Airtable's maximum page size
            if offset:
                params['offset'] = offset
            
            response = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('records', [])
            all_records.extend(records)
            
            logger.info(f"Fetched {len(records)} records (total so far: {len(all_records)})")
            
            # Check if there are more records to fetch
            if 'offset' in data:
                offset = data['offset']
            else:
                break
        
        logger.info(f"Successfully fetched {len(all_records)} total records from Airtable")
        return all_records
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching records from Airtable: {str(e)}")
        return []

def format_record_for_airtable(record: Dict) -> Dict:
    """
    Format a record dictionary to match Airtable's expected API structure.
    
    This function transforms a flat dictionary record into Airtable's required format
    with a 'fields' wrapper. It also maps common field names to Airtable field names
    and adds metadata like processing timestamp and source information.
    
    Enhanced to handle linked record arrays and preserve their structure when writing back.
    
    Args:
        record (Dict): Flat dictionary containing record data with field names as keys
    
    Returns:
        Dict: Airtable-formatted record with 'fields' wrapper and proper field mapping

    """
    fields = {}
    
    # Map fields to exact Airtable table format: Email, First Name, Last Name, Registrant Full Name (F), UID, NeonCRM Account ID, Circle Account ID (C), Phone, LinkedIn URL, Company, Title, Provider Type, Match Status, Match Reasons, Last Processed, Source
    field_mapping = {
        # Standard field mappings
        'email': 'Email',
        'Email': 'Email',
        'first_name': 'First Name',
        'First Name': 'First Name',
        'last_name': 'Last Name',
        'Last Name': 'Last Name',
        'name_full': 'Registrant Full Name (F)',
        'Registrant Full Name (F)': 'Registrant Full Name (F)',
        'uid': 'UID',
        'UID': 'UID',
        'uniqueID': 'UID',
        'neon_crm_id': 'NeonCRM Account ID',
        'NeonCRM Account ID': 'NeonCRM Account ID',
        'circle_id': 'Circle Account ID (C)',
        'Circle Account ID (C)': 'Circle Account ID (C)',
        #'phone': 'Phone',
        #'Phone': 'Phone',
        #'linkedin': 'LinkedIn URL',
        #'LinkedIn URL': 'LinkedIn URL',
        #'company': 'Company',
        #'Company': 'Company',
        #'title': 'Title',
        #'Title': 'Title',
        'provider_type': 'Provider Type',
        'Provider Type': 'Provider Type',
        'tags': 'Tags',
        'Tags': 'Tags',
        'tpg_id': 'TPG ID',
        'TPG ID': 'TPG ID',
        'member_status': 'Member Status',
        'Member Status': 'Member Status',
        'join_date': 'Join Date',
        'Join Date': 'Join Date',
        'event_rsvps': 'Event RSVPs',
        'Event RSVPs': 'Event RSVPs',
        'event_attendance': 'Event Attendance',
        'Event Attendance': 'Event Attendance',
        'donate_total': 'Donate(Total)',
        'Donate(Total)': 'Donate(Total)',
        'revenue_total': 'Revenue (Total)',
        'Revenue (Total)': 'Revenue (Total)',
        'newsletter': 'Newsletter',
        'Newsletter': 'Newsletter',
        'program_applications': 'Program Applications',
        'Program Applications': 'Program Applications',
        'program_acceptances': 'Program Acceptances',
        'Program Acceptances': 'Program Acceptances',
        'engagement_score': 'Engagement Score',
        'Engagement Score': 'Engagement Score',
        'test_link': 'Test Link',
        'Test Link': 'Test Link',
        'uid_from_test_link': 'UID (from Test Link)',
        'UID (from Test Link)': 'UID (from Test Link)',
        'tags_from_test_link': 'Tags (from Test Link)',
        'Tags (from Test Link)': 'Tags (from Test Link)',
        'events': 'Events',
        'Events': 'Events',
        'MATCH_STATUS': 'Match Status',
        'Match Status': 'Match Status',
        'match_status': 'Match Status',
        'MATCH_REASONS': 'Match Reasons',
        'Match Reasons': 'Match Reasons',
        'match_reasons': 'Match Reasons'
    }
    
    # Map fields and only include non-empty values
    for key, airtable_field in field_mapping.items():
        if key in record and record[key]:
            # Handle linked record arrays - preserve as arrays
            if isinstance(record[key], list):
                # Check if this looks like a linked record array (all items start with 'rec')
                if all(isinstance(item, str) and item.startswith('rec') for item in record[key]):
                    fields[airtable_field] = record[key]
                    logger.debug(f"Preserved linked record array for field '{airtable_field}': {record[key]}")
                else:
                    # Regular array - convert to string format
                    fields[airtable_field] = record[key]
            else:
                fields[airtable_field] = record[key]
    
    # Handle any fields that weren't in the mapping but might be linked records
    for key, value in record.items():
        if key not in field_mapping and value:
            # Check if this looks like a linked record array
            if isinstance(value, list) and all(isinstance(item, str) and item.startswith('rec') for item in value):
                fields[key] = value
                logger.debug(f"Preserved unmapped linked record field '{key}': {value}")
    
    # Add processing metadata for tracking
   # fields['Last Processed'] = datetime.now().strftime('%Y-%m-%d')  
   # fields['Source'] = 'Dedupe App'
    
    return {'fields': fields}


def update_airtable_with_results(records: List[Dict], table_name: str = None) -> bool:
    """
    Upload deduplication results to Airtable table.
    
    This is the main function for uploading processed deduplication results back to
    Airtable. It handles table validation, record formatting, and batch processing
    to efficiently upload large datasets while respecting Airtable's API limits.
    
    Args:
        records (List[Dict]): List of record dictionaries to upload
        table_name (str, optional): Target table name (defaults to "Deduplicated_Results")
    
    Returns:
        bool: True if upload was successful, False otherwise
    
    Features:
        - Validates table existence before upload
        - Processes records in batches of 10 (Airtable API limit)
        - Handles URL encoding for table names with spaces
        - Comprehensive error handling and logging
        - Uses table ID for "Deduplicated_Results" for reliability
    """
    if not validate_airtable_config():
        return False
    
    # Use table name directly (same approach as get_airtable_table_info)
    table = table_name or "Deduplicated_Results"
    url = f"{AIRTABLE_API_URL}/{table}"
    
    # Debug logging to see exact URL being used
    logger.info(f"DEBUG: AIRTABLE_BASE_ID = {AIRTABLE_BASE_ID}")
    logger.info(f"DEBUG: AIRTABLE_API_URL = {AIRTABLE_API_URL}")
    logger.info(f"DEBUG: Final URL = {url}")
    
    try:
        logger.info(f"Updating Airtable table {table} with {len(records)} records")
        
        # looging to check if the table exists by trying to get its info
        try:
            logger.info(f"Checking if table '{table}' exists at URL: {url}")
            check_response = requests.get(url, headers=AIRTABLE_HEADERS, params={'maxRecords': 1})
            logger.info(f"Table check response status: {check_response.status_code}")
            logger.info(f"Table check response text: {check_response.text[:200]}...")
            
            if check_response.status_code == 404:
                logger.error(f"Table '{table}' does not exist in Airtable")
                logger.info("Please create the 'Deduplicated_Results' table manually in Airtable")
                return False
            elif check_response.status_code != 200:
                logger.error(f"Unexpected status code when checking table: {check_response.status_code}")
                logger.error(f"Response: {check_response.text}")
                return False
            else:
                logger.info(f"Table '{table}' exists and is accessible")
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False
        
        # Format records for Airtable using the formatting function
        formatted_records = []
        for record in records:
            formatted_record = format_record_for_airtable(record)
            formatted_records.append(formatted_record)
        
        # Airtable API allows up to 10 records per request
        batch_size = 10
        success_count = 0
        
        # Process records in batches to respect API limits
        for i in range(0, len(formatted_records), batch_size):
            batch = formatted_records[i:i + batch_size]
            
            payload = {'records': batch}
            
            response = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
            response.raise_for_status()
            
            success_count += len(batch)
            logger.info(f"Successfully created {len(batch)} records in batch")
        
        logger.info(f"Successfully updated Airtable with {success_count} records")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating Airtable: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")
        return False

def clear_airtable_table(table_name: str = None) -> bool:
    """
    Clear all records from an Airtable table.
    
    This function removes all records from the specified table, useful for starting
    fresh with new deduplication results. It handles the deletion process in batches
    to respect Airtable's API limits and provides comprehensive error handling.
    
    Args:
        table_name (str, optional): Name of the table to clear (defaults to "Deduplicated_Results")
    
    Returns:
        bool: True if table was successfully cleared, False otherwise
 
    """
    if not validate_airtable_config():
        return False
    
    table = table_name or "Deduplicated_Results"
    url = f"{AIRTABLE_API_URL}/{table}"
    
    try:
        # First, get all record IDs that need to be deleted
        response = requests.get(url, headers=AIRTABLE_HEADERS)
        response.raise_for_status()
        
        data = response.json()
        records = data.get('records', [])
        
        if not records:
            logger.info("Table is already empty")
            return True
        
        # Delete records in batches to respect API limits
        record_ids = [record['id'] for record in records]
        batch_size = 10
        
        for i in range(0, len(record_ids), batch_size):
            batch_ids = record_ids[i:i + batch_size]
            
            for record_id in batch_ids:
                delete_url = f"{url}/{record_id}"
                response = requests.delete(delete_url, headers=AIRTABLE_HEADERS)
                response.raise_for_status()
        
        logger.info(f"Successfully cleared {len(record_ids)} records from table")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error clearing Airtable table: {str(e)}")
        return False

def handle_airtable_error(error: Exception) -> Dict:
    """
    Handle and log Airtable API errors with structured error information.
    
    This function provides consistent error handling across the module by capturing
    error details, timestamps, and error types. It's used to standardize error
    reporting and facilitate debugging.
    
    Args:
        error (Exception): The exception that occurred during Airtable operations
    
    Returns:
        Dict: Structured error information including message, timestamp, and error type
        
    """
    error_info = {
        'error': str(error),
        'timestamp': datetime.now().isoformat(),
        'type': type(error).__name__
    }
    
    logger.error(f"Airtable API error: {error_info}")
    
    return error_info

def get_airtable_table_info(table_name: str = None) -> Dict:
    """
    Get metadata and information about an Airtable table.
    
    This function retrieves basic information about a table including record count
    and field names. It's useful for monitoring table status and validating table
    structure before operations.
    
    Args:
        table_name (str, optional): Name of the table to inspect (defaults to AIRTABLE_TABLE_NAME)
    
    Returns:
        Dict: Table information including name, record count, and field names.
              Returns empty dict if configuration is invalid or API call fails.
              
    """
    if not validate_airtable_config():
        return {}
    
    table = table_name or AIRTABLE_TABLE_NAME
    url = f"{AIRTABLE_API_URL}/{table}"
    
    try:
        response = requests.get(url, headers=AIRTABLE_HEADERS, params={'maxRecords': 1})
        response.raise_for_status()
        
        data = response.json()
        return {
            'table_name': table,
            'record_count': len(data.get('records', [])),
            'fields': list(data.get('records', [{}])[0].get('fields', {}).keys()) if data.get('records') else []
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting table info: {str(e)}")
        return {}

def detect_linked_record_fields(records: List[Dict]) -> Set[str]:
    """
    Detect which fields in Airtable records contain linked record arrays.
    
    This function analyzes the structure of Airtable records to identify fields
    that contain linked record arrays (lists of record IDs) rather than scalar values.
    It combines both automatic detection and configured field names.
    
    Args:
        records (List[Dict]): List of Airtable records in native format
    
    Returns:
        Set[str]: Set of field names that contain linked record arrays
    """
    linked_record_fields = set(LINKED_RECORD_FIELDS)  # Start with configured fields
    
    for record in records:
        fields = record.get('fields', {})
        for field_name, field_value in fields.items():
            # Check if the field value is a list (linked record array)
            if isinstance(field_value, list):
                # Additional check: if all items in the list look like record IDs
                # Airtable record IDs typically start with 'rec' and are alphanumeric
                if all(isinstance(item, str) and item.startswith('rec') for item in field_value):
                    linked_record_fields.add(field_name)
    
    return linked_record_fields

def convert_airtable_to_dataframe_format(records: List[Dict]) -> List[Dict]:
    """
    Convert Airtable records to the format expected by deduplication functions.
    
    This function transforms Airtable's native record format (with 'id' and 'fields')
    into a flat dictionary format that the deduplication pipeline expects. It maps
    Airtable field names back to the internal field names used by the application.
    
    Enhanced to preserve linked record arrays instead of converting them to strings.
    
    Args:
        records (List[Dict]): List of Airtable records in native format
    
    Returns:
        List[Dict]: List of flat dictionaries ready for DataFrame conversion

    """
    converted_records = []
    
    # Detect which fields contain linked records
    linked_record_fields = detect_linked_record_fields(records)
    if linked_record_fields:
        logger.info(f"Detected linked record fields: {linked_record_fields}")
    
    for record in records:
        fields = record.get('fields', {})
        converted_record = {
            'id': record.get('id'),
            'email': fields.get('Email', ''),
            'first_name': fields.get('First Name', ''),
            'last_name': fields.get('Last Name', ''),
            'name_full': fields.get('Registrant Full Name (F)', ''),
            'uid': fields.get('UID', ''),
            'neon_crm_id': fields.get('NeonCRM Account ID', ''),
            'circle_id': fields.get('Circle Account ID (C)', ''),
            #'phone': fields.get('Phone', ''),
            #'linkedin': fields.get('LinkedIn URL', ''),
            #'company': fields.get('Company', ''),
            #'title': fields.get('Title', ''),
            'provider_type': fields.get('Provider Type', ''),
            'tags': fields.get('Tags', ''),
            'tpg_id': fields.get('TPG ID', ''),
            'member_status': fields.get('Member Status', ''),
            'join_date': fields.get('Join Date', ''),
            'event_rsvps': fields.get('Event RSVPs', ''),
            'event_attendance': fields.get('Event Attendance', ''),
            'donate_total': fields.get('Donate(Total)', ''),
            'revenue_total': fields.get('Revenue (Total)', ''),
            'newsletter': fields.get('Newsletter', ''),
            'program_applications': fields.get('Program Applications', ''),
            'program_acceptances': fields.get('Program Acceptances', ''),
            'engagement_score': fields.get('Engagement Score', ''),
            'test_link': fields.get('Test Link', ''),
            'uid_from_test_link': fields.get('UID (from Test Link)', ''),
            'tags_from_test_link': fields.get('Tags (from Test Link)', ''),
            'events': fields.get('Events', ''),
            'source': 'airtable'
        }
        
        # Handle linked record fields - preserve as arrays
        for field_name, field_value in fields.items():
            if field_name in linked_record_fields:
                # Preserve linked record arrays as-is
                converted_record[field_name] = field_value
                logger.debug(f"Preserved linked record field '{field_name}': {field_value}")
        
        converted_records.append(converted_record)
    
    return converted_records

def process_airtable_deduplication() -> Dict:
    """
    Main orchestrator function for Airtable-based deduplication process.
    
    This function serves as the primary entry point for Airtable deduplication.
    It coordinates the entire process: fetching records from Airtable, converting
    them to the appropriate format, and returning them for processing by the
    deduplication pipeline.
    
    Returns:
        Dict: Result dictionary containing success status, records, and count.
              On error, contains error information and details.
              
    Process Flow:
        1. Fetch records from "All Providers" table
        2. Convert records to DataFrame-compatible format
        3. Return records for deduplication processing
        4. Handle errors and provide detailed error information
    
    """
    try:
        logger.info("Starting Airtable deduplication process")
        
        # Step 1: Fetch records from "All Providers" table
        airtable_records = fetch_airtable_records()
        if not airtable_records:
            return {'error': 'No records found in Airtable table'}
        
        converted_records = convert_airtable_to_dataframe_format(airtable_records)
        return {
            'success': True,
            'records': converted_records,
            'count': len(converted_records)
        }
        
    except Exception as e:
        error_info = handle_airtable_error(e)
        return {'error': str(e), 'details': error_info} 

def test_deduplicated_results_table_access() -> Dict:
    """
    Test function to verify access to the Deduplicated_Results table.
    
    This function specifically tests the same URL construction and access pattern
    used by update_airtable_with_results to help debug table access issues.
    
    Returns:
        Dict: Test results including success status and details
    """
    if not validate_airtable_config():
        return {'success': False, 'error': 'Invalid configuration'}
    
    table = "Deduplicated_Results"
    url = f"{AIRTABLE_API_URL}/{table}"
    
    try:
        logger.info(f"Testing table access for: {table}")
        logger.info(f"URL: {url}")
        logger.info(f"DEBUG: AIRTABLE_BASE_ID = {AIRTABLE_BASE_ID}")
        logger.info(f"DEBUG: AIRTABLE_API_URL = {AIRTABLE_API_URL}")
        
        # Test GET request (same as table existence check)
        response = requests.get(url, headers=AIRTABLE_HEADERS, params={'maxRecords': 1})
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response text: {response.text[:500]}...")
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            fields = list(records[0].get('fields', {}).keys()) if records else []
            
            return {
                'success': True,
                'table_name': table,
                'url': url,
                'record_count': len(records),
                'fields': fields,
                'message': 'Table access successful'
            }
        else:
            return {
                'success': False,
                'table_name': table,
                'url': url,
                'status_code': response.status_code,
                'response_text': response.text,
                'error': f'HTTP {response.status_code}: {response.text}'
            }
            
    except Exception as e:
        return {
            'success': False,
            'table_name': table,
            'url': url,
            'error': str(e),
            'exception_type': type(e).__name__
        } 

def configure_linked_record_fields(field_names: List[str]) -> None:
    """
    Configure which fields should be treated as linked records.
    
    This function allows you to specify field names that should preserve their
    linked record structure instead of being converted to strings.
    
    Args:
        field_names (List[str]): List of field names to treat as linked records
    """
    global LINKED_RECORD_FIELDS
    LINKED_RECORD_FIELDS.update(field_names)
    logger.info(f"Configured linked record fields: {LINKED_RECORD_FIELDS}")

def get_linked_record_fields() -> Set[str]:
    """
    Get the current list of configured linked record fields.
    
    Returns:
        Set[str]: Set of field names configured as linked records
    """
    return LINKED_RECORD_FIELDS.copy()

def standardize_airtable_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize Airtable data using the reusable standardization function.
    
    This function uses the same standardization logic as CSV processing
    to ensure consistent data processing across all sources.
    
    Args:
        df (pd.DataFrame): Raw DataFrame from Airtable data
    
    Returns:
        pd.DataFrame: DataFrame with standardized fields (_std suffix)
    """
    logger.info(f"Starting standardization of {len(df)} records using reusable function")
    
    # Import the reusable standardization function
    from app.utils.data_processor import standardize_dataframe
    
    # Create field mappings for Airtable data
    field_mappings = {
        'email': 'email' if 'email' in df.columns else None,
        'first_name': 'first_name' if 'first_name' in df.columns else None,
        'last_name': 'last_name' if 'last_name' in df.columns else None,
        'name_full': 'name_full' if 'name_full' in df.columns else None,
        'linkedin': 'linkedin' if 'linkedin' in df.columns else None,
        'uniqueID': 'uid' if 'uid' in df.columns else None,
        'phone_cols': ['phone'] if 'phone' in df.columns else []
    }
    
    # Use the reusable standardization function
    df = standardize_dataframe(df, field_mappings)
    
    # Add source field
    df['source'] = 'airtable'
    
    logger.info(f"Standardization complete. New columns: {[col for col in df.columns if col.endswith('_std')]}")
    
    return df

 