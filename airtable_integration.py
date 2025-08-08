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
        
        # Process checkbox fields to convert linked records to event names
        converted_record = process_checkbox_fields(converted_record)
        
        converted_records.append(converted_record)
    
    return converted_records



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

def get_event_names_from_ids(event_ids: List[str]) -> List[str]:
    """
    Convert linked record IDs to event names by querying the Events table.
    
    Args:
        event_ids (List[str]): List of linked record IDs (e.g., ['rec123', 'rec456'])
    
    Returns:
        List[str]: List of event names
    """
    if not event_ids or not validate_airtable_config():
        return []
    
    event_names = []
    
    try:
        # Query the Events table for each event ID
        for event_id in event_ids:
            if event_id.startswith('rec'):  # Valid Airtable record ID
                url = f"{AIRTABLE_API_URL}/Events/{event_id}"
                response = requests.get(url, headers=AIRTABLE_HEADERS)
                
                if response.status_code == 200:
                    event_data = response.json()
                    event_name = event_data.get('fields', {}).get('Event Name', '')
                    if event_name:
                        event_names.append(event_name)
                else:
                    logger.warning(f"Could not fetch event with ID {event_id}: {response.status_code}")
        
        logger.info(f"Converted {len(event_ids)} event IDs to {len(event_names)} event names")
        return event_names
        
    except Exception as e:
        logger.error(f"Error converting event IDs to names: {str(e)}")
        return []

def process_checkbox_fields(record: Dict) -> Dict:
    """
    Process checkbox fields and convert boolean values to event names from linked records.
    
    Args:
        record (Dict): Record with checkbox fields
    
    Returns:
        Dict: Record with processed checkbox fields
    """
    processed_record = record.copy()
    
    # Get the linked events from the record (these contain actual event names)
    linked_events = record.get('Events', [])  # "Events" column with linked records
    if isinstance(linked_events, list) and all(isinstance(item, str) and item.startswith('rec') for item in linked_events):
        # Convert linked record IDs to event names
        event_names = get_event_names_from_ids(linked_events)
        logger.debug(f"Found linked events: {linked_events} → {event_names}")
    elif isinstance(linked_events, str) and linked_events.strip():
        # Handle string format (comma-separated)
        event_names = [name.strip() for name in linked_events.split(',') if name.strip()]
        logger.debug(f"Found event names from string: {event_names}")
    else:
        event_names = []
    
    # Process Event RSVPs checkbox
    event_rsvps_checkbox = record.get('Event RSVPs', False)
    if isinstance(event_rsvps_checkbox, bool) and event_rsvps_checkbox and event_names:
        # If checkbox is checked AND we have event names, use the event names
        processed_record['Event RSVPs'] = event_names
        logger.debug(f"Converted boolean Event RSVPs checkbox: {event_rsvps_checkbox} → {event_names}")
    elif isinstance(event_rsvps_checkbox, bool) and event_rsvps_checkbox and not event_names:
        # If checkbox is checked but no event names, use placeholder
        processed_record['Event RSVPs'] = ['Has RSVP\'d to Events']
        logger.debug(f"Converted boolean Event RSVPs checkbox: {event_rsvps_checkbox} → ['Has RSVP'd to Events'] (no linked events)")
    elif isinstance(event_rsvps_checkbox, list) and all(isinstance(item, str) and item.startswith('rec') for item in event_rsvps_checkbox):
        # Handle direct linked record IDs
        event_names = get_event_names_from_ids(event_rsvps_checkbox)
        processed_record['Event RSVPs'] = event_names
        logger.debug(f"Converted Event RSVPs linked records: {event_rsvps_checkbox} → {event_names}")
    elif isinstance(event_rsvps_checkbox, str) and event_rsvps_checkbox.strip():
        # Handle string format (comma-separated)
        event_names = [name.strip() for name in event_rsvps_checkbox.split(',') if name.strip()]
        processed_record['Event RSVPs'] = event_names
    
    # Process Event Attendance checkbox
    event_attendance_checkbox = record.get('Event Attendance', False)
    if isinstance(event_attendance_checkbox, bool) and event_attendance_checkbox and event_names:
        # If checkbox is checked AND we have event names, use the event names
        processed_record['Event Attendance'] = event_names
        logger.debug(f"Converted boolean Event Attendance checkbox: {event_attendance_checkbox} → {event_names}")
    elif isinstance(event_attendance_checkbox, bool) and event_attendance_checkbox and not event_names:
        # If checkbox is checked but no event names, use placeholder
        processed_record['Event Attendance'] = ['Has Attended Events']
        logger.debug(f"Converted boolean Event Attendance checkbox: {event_attendance_checkbox} → ['Has Attended Events'] (no linked events)")
    elif isinstance(event_attendance_checkbox, list) and all(isinstance(item, str) and item.startswith('rec') for item in event_attendance_checkbox):
        # Handle direct linked record IDs
        event_names = get_event_names_from_ids(event_attendance_checkbox)
        processed_record['Event Attendance'] = event_names
        logger.debug(f"Converted Event Attendance linked records: {event_attendance_checkbox} → {event_names}")
    elif isinstance(event_attendance_checkbox, str) and event_attendance_checkbox.strip():
        # Handle string format (comma-separated)
        event_names = [name.strip() for name in event_attendance_checkbox.split(',') if name.strip()]
        processed_record['Event Attendance'] = event_names
    
    return processed_record

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




def smart_update_all_providers_with_deduplication() -> Dict:
    """
    Smart deduplication that updates existing records in "All Providers" table
    and deletes duplicate records. This preserves comments and history for primary records.
    
    Returns:
        Dict: Results of the smart deduplication process
    """
    if not validate_airtable_config():
        return {'error': 'Airtable configuration invalid'}
    
    try:
        logger.info("Starting smart deduplication in All Providers table")
        
        # Step 1: Fetch existing records from "All Providers" table
        existing_records = fetch_airtable_records("All Providers")
        if not existing_records:
            logger.warning("No existing records found in All Providers table")
            return {'error': 'No existing records found in All Providers table'}
        
        logger.info(f"Found {len(existing_records)} existing records in All Providers")
        
        # Step 2: Convert to DataFrame format for deduplication
        records_data = convert_airtable_to_dataframe_format(existing_records)
        df = pd.DataFrame(records_data)
        
        # Step 3: Standardize the data (same as existing logic)
        df = standardize_airtable_data(df)
        
        # Step 4: Run existing deduplication logic
        from app.utils.matcher import create_matched_groups
        from app.utils.merger import merge_matched_records
        
        logger.info(f"Starting deduplication on {len(df)} records")
        matched_groups, merge_reasons = create_matched_groups(df)
        logger.info(f"Deduplication found {len(matched_groups)} groups")
        
        # Debug: Show group sizes
        group_sizes = [len(group['indices']) for group in matched_groups]
        logger.info(f"Group sizes: {group_sizes}")
        logger.info(f"Largest group has {max(group_sizes) if group_sizes else 0} records")
        
        final_df = merge_matched_records(df, matched_groups, merge_reasons)
        
        # Step 5: Convert back to records format and filter out internal fields
        final_records = final_df.to_dict('records')
        
        # Debug: Show field names in first record
        if final_records:
            logger.info(f"Sample deduplicated record fields: {list(final_records[0].keys())}")
        
        # Filter out internal deduplication fields that don't exist in Airtable
        internal_fields = {'MATCH_STATUS', 'MATCH_REASONS', 'email_std', 'linkedin_url_std', 'uniqueID_std', 'phone_set_std', 'first_name_std', 'last_name_std'}
        filtered_records = []
        
        for record in final_records:
            filtered_record = {k: v for k, v in record.items() if k not in internal_fields}
            filtered_records.append(filtered_record)
        
        # Debug: Show field names in first filtered record
        if filtered_records:
            logger.info(f"Sample filtered record fields: {list(filtered_records[0].keys())}")
        
        logger.info(f"Deduplication complete: {len(existing_records)} → {len(filtered_records)} records")
        
        # Debug: Show merge reasons for first few groups
        if merge_reasons:
            logger.info(f"Sample merge reasons: {list(merge_reasons.items())[:5]}")
        
        # Step 6: Update existing records with merged data and track which ones to delete
        success, records_to_delete = update_existing_records_with_deduplication_results(filtered_records, existing_records, matched_groups, "All Providers")
        
        if success:
            # Step 7: Delete duplicate records
            if records_to_delete:
                delete_success = delete_duplicate_records(records_to_delete, "All Providers")
                if delete_success:
                    logger.info(f"Successfully deleted {len(records_to_delete)} duplicate records")
                else:
                    logger.warning("Failed to delete some duplicate records")
            else:
                logger.info("No duplicate records to delete")
            
            logger.info("Successfully updated All Providers table with deduplicated results")
            return {
                'success': True,
                'original_records': len(existing_records),
                'final_records': len(filtered_records),
                'records_updated': len(filtered_records),
                'records_deleted': len(records_to_delete),
                'records_removed': len(existing_records) - len(filtered_records)
            }
        else:
            logger.error("Failed to update All Providers table")
            return {'error': 'Failed to update All Providers table'}
        
    except Exception as e:
        logger.error(f"Error in smart deduplication: {str(e)}")
        return {'error': str(e)}


def update_existing_records_with_deduplication_results(records: List[Dict], existing_records: List[Dict], matched_groups: List[Dict], table_name: str = "All Providers") -> tuple[bool, List[str]]:
    """
    Update existing records in Airtable table using actual deduplication results.
    
    Args:
        records: List of deduplicated records to update
        existing_records: List of existing records from Airtable
        matched_groups: Results from deduplication showing which records are duplicates
        table_name: Name of the table to update
    
    Returns:
        tuple: (success: bool, records_to_delete: List[str])
    """
    if not validate_airtable_config():
        return False, []
    
    url = f"{AIRTABLE_API_URL}/{table_name}"
    
    try:
        logger.info(f"Updating existing records in {table_name} table using deduplication results")
        
        # Track which records are being updated (primary records from deduplication)
        updated_record_ids = set()
        records_to_delete = []
        records_to_create = []
        
        # Process deduplicated records in batches
        batch_size = 10
        success_count = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_updates = []
            
            for record_idx, record in enumerate(batch):
                # Calculate the actual index in the full deduplicated records list
                actual_record_idx = i * batch_size + record_idx
                
                # Safety check - make sure we don't exceed the number of deduplicated records
                if actual_record_idx >= len(records):
                    logger.warning(f"Skipping record index {actual_record_idx} - exceeds deduplicated records count ({len(records)})")
                    continue
                
                # Find the corresponding matched group to determine which existing record to update
                if actual_record_idx < len(matched_groups):
                    group = matched_groups[actual_record_idx]
                    # Use the first record in the group as the primary (the one to update)
                    original_record_index = group['indices'][0]
                    
                    if original_record_index < len(existing_records):
                        # Update the primary record from this group
                        existing_record = existing_records[original_record_index]
                        existing_record_id = existing_record['id']
                        
                        formatted_record = format_record_for_airtable(record)
                        fields = formatted_record.get('fields', {})
                        batch_updates.append({
                            'id': existing_record_id,
                            'fields': fields
                        })
                        updated_record_ids.add(existing_record_id)
                        logger.info(f"Updating primary record {existing_record_id} with deduplicated data")
                    else:
                        logger.error(f"Original record index {original_record_index} out of bounds for deduplicated record {actual_record_idx}")
                        continue
                else:
                    logger.error(f"No matched group found for deduplicated record {actual_record_idx}")
                    continue
            
            if batch_updates:
                # Update records in batch
                payload = {'records': batch_updates}
                response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
                response.raise_for_status()
                
                success_count += len(batch_updates)
                logger.info(f"Successfully updated {len(batch_updates)} records in batch")
        
        # No new records should be created - all deduplicated records should update existing records
        if records_to_create:
            logger.error(f"Unexpected: {len(records_to_create)} records were marked for creation but should not be")
        
        # Find records to delete (only the duplicates within each group)
        for group in matched_groups:
            indices = group['indices']
            if len(indices) > 1:  # Only groups with duplicates
                # Keep the first record (primary), delete the rest
                primary_index = indices[0]
                duplicate_indices = indices[1:]  # All except the first
                
                for duplicate_index in duplicate_indices:
                    if duplicate_index < len(existing_records):
                        duplicate_record = existing_records[duplicate_index]
                        records_to_delete.append(duplicate_record['id'])
        
        logger.info(f"Successfully updated {success_count} existing records")
        logger.info(f"Found {len(records_to_delete)} records to delete")
        return True, records_to_delete
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating existing records: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response text: {e.response.text}")
        return False, []


def create_new_records_batch(records: List[Dict], table_name: str = "All Providers") -> bool:
    """
    Create new records in Airtable table in batches.
    
    Args:
        records: List of records to create
        table_name: Name of the table to create records in
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not validate_airtable_config():
        return False
    
    url = f"{AIRTABLE_API_URL}/{table_name}"
    
    try:
        logger.info(f"Creating {len(records)} new records in {table_name}")
        
        # Process records in batches
        batch_size = 10
        success_count = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_creates = []
            
            for record in batch:
                formatted_record = format_record_for_airtable(record)
                batch_creates.append(formatted_record)
            
            if batch_creates:
                # Create records in batch
                payload = {'records': batch_creates}
                response = requests.post(url, headers=AIRTABLE_HEADERS, json=payload)
                response.raise_for_status()
                
                success_count += len(batch_creates)
                logger.info(f"Successfully created {len(batch_creates)} records in batch")
        
        logger.info(f"Successfully created {success_count} new records")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating new records: {str(e)}")
        return False


def delete_duplicate_records(record_ids: List[str], table_name: str = "All Providers") -> bool:
    """
    Delete duplicate records from Airtable table.
    
    Args:
        record_ids: List of record IDs to delete
        table_name: Name of the table to delete from
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not validate_airtable_config():
        return False
    
    url = f"{AIRTABLE_API_URL}/{table_name}"
    
    try:
        logger.info(f"Deleting {len(record_ids)} duplicate records from {table_name}")
        
        # Delete records in batches
        batch_size = 10
        success_count = 0
        
        for i in range(0, len(record_ids), batch_size):
            batch_ids = record_ids[i:i + batch_size]
            
            for record_id in batch_ids:
                delete_url = f"{url}/{record_id}"
                response = requests.delete(delete_url, headers=AIRTABLE_HEADERS)
                response.raise_for_status()
                success_count += 1
            
            logger.info(f"Successfully deleted {len(batch_ids)} records in batch")
        
        logger.info(f"Successfully deleted {success_count} duplicate records")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error deleting duplicate records: {str(e)}")
        return False





 

 