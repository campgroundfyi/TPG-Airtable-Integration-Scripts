# Airtable Integration System Walkthrough

This document explains how the Airtable integration system works, specifically focusing on two key files: `airtable_integration.py` (Python backend) and `all_providers.js` (Airtable script).

## **System Overview**

The system consists of two main components that work together to handle Airtable data with linked record preservation:

1. **`all_providers.js`** - Airtable script that consolidates data from multiple tables
2. **`airtable_integration.py`** - Python backend that processes and preserves linked records

## **File 1: all_providers.js (Airtable Script)**

### **Purpose**
This script runs directly in Airtable to consolidate data from multiple source tables into a single "All Providers" table while preserving linked record relationships.

### **How It Works**

#### **Step 1: Setup and Cleanup**
```javascript
let destinationTable = base.getTable("All Providers");
let existingRecords = await destinationTable.selectRecordsAsync();

// Delete existing records in batches
for (let record of existingRecords.records) {
    await destinationTable.deleteRecordAsync(record.id);
}
```
- **Clears the destination table** to start fresh
- **Prevents duplicate data** from previous runs

#### **Step 2: Configuration**
```javascript
let linkedRecordFields = [
    'Events',  // Fields that should preserve linked record structure
    'Test Link',
];

let idFields = [
   "UID", "Email", "First Name", "Last Name", 
   "Events", // Standard fields to extract
   // ... more fields
];
```
- **`linkedRecordFields`**: Specifies which fields should preserve linked record arrays
- **`idFields`**: Lists all fields to extract from source tables

#### **Step 3: Table Processing**
```javascript
for (let table of base.tables) {
   if (table.name === destinationTable.name || 
       table.name === "Events" ||
       table.name === "Deduplicated_Results") continue;
```
- **Loops through all tables** in the Airtable base
- **Excludes specific tables** (destination, Events, results tables)
- **Only processes tables** that contain provider data

#### **Step 4: Linked Record Preservation**
```javascript
if (linkedRecordFields.includes(field) || 
    cellValue.every(item => item && item.id && item.id.startsWith('rec'))) {
    // Preserve linked records as array of objects with id property
    newRecord[field] = cellValue.map(item => ({id: item.id}));
}
```
- **Detects linked record fields** automatically
- **Preserves the linked record structure** as `[{id: 'rec123'}, {id: 'rec456'}]`
- **Maintains relationships** between records

### **Key Features**
- **Automatic linked record detection**
- **Preserves linked record arrays** instead of converting to strings
- **Handles both single and multiple linked records**
- **Excludes unwanted tables** from processing
- **Batch processing** for large datasets

---

## **File 2: airtable_integration.py (Python Backend)**

### **Purpose**
This Python module handles the communication between your application and Airtable, with special focus on preserving linked record structures throughout the data pipeline.

### **How It Works**

#### **Step 1: Linked Record Detection**
```python
def detect_linked_record_fields(records: List[Dict]) -> Set[str]:
    linked_record_fields = set(LINKED_RECORD_FIELDS)  # Start with configured fields
    
    for record in records:
        fields = record.get('fields', {})
        for field_name, field_value in fields.items():
            if isinstance(field_value, list):
                if all(isinstance(item, str) and item.startswith('rec') for item in field_value):
                    linked_record_fields.add(field_name)
    
    return linked_record_fields
```
- **Automatically detects** fields containing linked record arrays
- **Looks for arrays** where all items start with 'rec' (Airtable record ID format)
- **Combines automatic detection** with configured fields

#### **Step 2: Data Conversion**
```python
def convert_airtable_to_dataframe_format(records: List[Dict]) -> List[Dict]:
    linked_record_fields = detect_linked_record_fields(records)
    
    for record in records:
        fields = record.get('fields', {})
        converted_record = {
            'email': fields.get('Email', ''),
            'first_name': fields.get('First Name', ''),
            # ... standard fields
        }
        
        # Handle linked record fields - preserve as arrays
        for field_name, field_value in fields.items():
            if field_name in linked_record_fields:
                converted_record[field_name] = field_value  # Preserve as-is
```
- **Converts Airtable format** to application format
- **Preserves linked record arrays** instead of converting to strings
- **Maintains data structure** for further processing

#### **Step 3: Writing Back to Airtable**
```python
def format_record_for_airtable(record: Dict) -> Dict:
    for key, airtable_field in field_mapping.items():
        if key in record and record[key]:
            if isinstance(record[key], list):
                if all(isinstance(item, str) and item.startswith('rec') for item in record[key]):
                    fields[airtable_field] = record[key]  # Preserve linked record array
```
- **Formats data** for Airtable API
- **Preserves linked record arrays** when writing back
- **Handles both mapped and unmapped** linked record fields

### **Key Features**
- **Automatic linked record detection**
- **Configuration system** for custom linked record fields
- **Preserves linked record structure** throughout the pipeline
- **Handles both reading and writing** to Airtable
- **Comprehensive error handling**

---

## **How They Work Together**

### **Data Flow**
1. **`all_providers.js`** consolidates data from multiple tables into "All Providers"
2. **`airtable_integration.py`** reads from "All Providers" and processes the data
3. **Linked records are preserved** throughout the entire process
4. **Results are written back** to Airtable with relationships intact

### **Example Scenario**
```
Source Table: "Circle Members"
├── Name: "John Doe"
├── Email: "john@example.com"
└── Events: [recEvent123, recEvent456]  ← Linked record array

↓ all_providers.js processes this

All Providers Table:
├── Name: "John Doe"
├── Email: "john@example.com"
├── Provider Type: "Circle Members"
└── Events: [{id: 'recEvent123'}, {id: 'recEvent456'}]  ← Preserved as array

↓ airtable_integration.py processes this

Application Data:
├── name: "John Doe"
├── email: "john@example.com"
└── events: ['recEvent123', 'recEvent456']  ← Still preserved as array

↓ Results written back to Airtable

Deduplicated_Results Table:
├── Name: "John Doe"
├── Email: "john@example.com"
└── Events: [{id: 'recEvent123'}, {id: 'recEvent456'}]  ← Relationships intact
```


---

## **Configuration**

### **Adding New Linked Record Fields**
```python
# In airtable_integration.py
LINKED_RECORD_FIELDS = {
    'Events',
    'Test Link',
    'Your New Field',  # Add here
}
```

```javascript
// In all_providers.js
let linkedRecordFields = [
    'Events',
    'Test Link',
    'Your New Field',  // Add here
];
```

### **Excluding Tables**
```javascript
// In all_providers.js
if (table.name === destinationTable.name || 
    table.name === "Events" ||
    table.name === "Your Table to Exclude") continue;
```

---

##  **Getting Started**

1. **Set up your Airtable base** with the required tables
2. **Run the `all_providers.js` script** to consolidate data
3. **Use the `airtable_integration.py` module** in your application
4. **Configure linked record fields** as needed
5. **Test the system** with your data

This system ensures that your Airtable linked records are preserved throughout the entire data processing pipeline! 🎉 
