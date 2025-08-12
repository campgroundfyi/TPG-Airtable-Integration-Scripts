# Dedupe API - How to Use

## What This API Does

This API endpoint automatically deduplicates (removes duplicates) and merges records from your Airtable base. It finds similar records and combines them into single, clean entries.

## API Process: Input → Processing → Output

### **What the API Takes In (Input)**

**Data Sources:**
- **Airtable Tables**: All configured tables in your base
- **CSV Files**: Any additional data files you've uploaded
- **Provider Records**: Contact information, company details, etc.

**Required:**
- **API Key**: For authentication (in Authorization header)
- **No Request Body**: The API doesn't need any data in the request

**What Happens Automatically:**
- Fetches all data from your configured Airtable tables
- Combines multiple data sources into one dataset
- Analyzes existing records for potential duplicates

### ⚙**What the API Does (Processing)**

**Step 1: Data Collection & Analysis**
- Connects to your Airtable base
- Downloads all records from configured tables
- Combines data from multiple sources
- Creates a unified dataset for processing

**Step 2: Duplicate Detection**
- **Email Matching**: Finds records with similar email addresses
- **Phone Matching**: Identifies records with matching phone numbers
- **Name Matching**: Uses fuzzy logic to find similar names
- **LinkedIn Matching**: Matches records with same LinkedIn profiles
- **ID Matching**: Groups records with similar unique identifiers

**Step 3: Record Grouping**
- Groups similar records together
- Applies business logic for record linking
- Handles edge cases and data inconsistencies
- Creates clusters of potential duplicates

**Step 4: Smart Merging**
- Automatically merges duplicate records
- Resolves conflicts intelligently
- Preserves the most complete information
- Updates existing records instead of creating new ones

**Step 5: Data Update**
- Updates your Airtable base with clean data
- Maintains data relationships and integrity
- Logs all changes and processing results

### **What the API Returns (Output)**

**Important: The API does NOT return the actual deduplicated data!**

The API only returns a JSON summary of what happened. The actual deduplication happens directly in your Airtable base.

**Immediate Response:**
```json
{
  "success": true,
  "message": "Smart deduplication completed successfully",
  "original_records": 150,
  "final_records": 120,
  "records_updated": 30,
  "records_created": 0,
  "records_removed": 0
}
```

**What Each Field Means:**
- `original_records`: How many records existed before deduplication
- `final_records`: How many records exist after deduplication
- `records_updated`: How many records were modified/merged
- `records_created`: New records created (usually 0)
- `records_removed`: Records removed (usually 0)

### **How the Data Actually Flows**

**The API works in 4 steps:**

1. ** READ from Airtable**: Fetches existing records from your "All Providers" table
2. ** PROCESS in Memory**: Runs deduplication algorithms locally (no CSV output)
3. ** UPDATE Airtable**: Directly modifies your Airtable base with cleaned data
4. ** RETURN Summary**: Sends back JSON with just the statistics

### **Real Example of the Process**

**Input Data (Before API Call):**
```
Table: Providers
├── Record 1: john.smith@email.com, Phone: 555-0123
├── Record 2: johnsmith@email.com, LinkedIn: linkedin.com/in/johnsmith  
├── Record 3: j.smith@email.com, Company: ABC Corp
└── Record 4: sarah.jones@email.com, Phone: 555-0456
```

**Processing:**
1. **Detection**: Finds Records 1, 2, and 3 are the same person
2. **Grouping**: Groups them into a duplicate cluster
3. **Merging**: Combines all information into one record
4. **Update**: Replaces the 3 duplicate records with 1 clean record

**Output Data (After API Call):**
```
Table: Providers (Updated)
├── Record 1: john.smith@email.com
│   ├── Phone: 555-0123
│   ├── LinkedIn: linkedin.com/in/johnsmith
│   └── Company: ABC Corp
└── Record 2: sarah.jones@email.com, Phone: 555-0456
```

**Result:**
- **Before**: 4 records (3 duplicates)
- **After**: 2 records (no duplicates)
- **Records Updated**: 1 (the merged John Smith record)
- **Records Removed**: 2 (duplicates eliminated)

## API Endpoint

```
POST /api/trigger-dedupe
```

**Base URL**: `https://your-domain.com` (replace with your actual domain)

## Authentication

**Required**: API Key in the Authorization header

```
Authorization: your-api-key-here
```

## How to Call the API

### Using cURL
```bash
curl -X POST "https://your-domain.com/api/trigger-dedupe" \
  -H "Authorization: your-api-key-here" \
  -H "Content-Type: application/json"
```

### Using JavaScript/Fetch
```javascript
fetch('https://your-domain.com/api/trigger-dedupe', {
  method: 'POST',
  headers: {
    'Authorization': 'your-api-key-here',
    'Content-Type': 'application/json'
  }
})
.then(response => response.json())
.then(data => console.log(data));
```

### Using Python/Requests
```python
import requests

url = "https://your-domain.com/api/trigger-dedupe"
headers = {
    "Authorization": "your-api-key-here",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers)
data = response.json()
print(data)
```

## What Happens When You Call the API

1. **Fetches all data** from your configured Airtable tables
2. **Analyzes records** to find potential duplicates using:
   - Email addresses
   - Phone numbers
   - Names (with fuzzy matching)
   - LinkedIn profiles
   - Unique identifiers
3. **Groups similar records** together
4. **Merges duplicates** into single records
5. **Updates your Airtable base** with the clean data
6. **Returns results** showing what was processed

## API Response

### Success Response (200)
```json
{
  "success": true,
  "message": "Smart deduplication completed successfully",
  "original_records": 150,
  "final_records": 120,
  "records_updated": 30,
  "records_created": 0,
  "records_removed": 0
}
```

**Response Fields:**
- `original_records`: Total records before deduplication
- `final_records`: Total records after deduplication
- `records_updated`: Records that were modified/merged
- `records_created`: New records created (usually 0)
- `records_removed`: Records removed (usually 0)

### Error Responses

**400 Bad Request**
```json
{
  "error": "Configuration error or deduplication failed"
}
```

**401 Unauthorized**
```json
{
  "error": "Invalid or missing API key"
}
```

**500 Internal Server Error**
```json
{
  "error": "Smart deduplication failed"
}
```

## Processing Time

- **Small datasets** (< 1000 records): 1-2 minutes
- **Medium datasets** (1000-10000 records): 2-5 minutes
- **Large datasets** (> 10000 records): 5-10 minutes

## Example: What the API Actually Does

**Before API Call:**
Your Airtable has 3 separate records for the same person:
- Record 1: john.smith@email.com, Phone: 555-0123
- Record 2: johnsmith@email.com, LinkedIn: linkedin.com/in/johnsmith
- Record 3: j.smith@email.com, Company: ABC Corp

**After API Call:**
Your Airtable now has 1 clean record:
- john.smith@email.com
  - Phone: 555-0123
  - LinkedIn: linkedin.com/in/johnsmith
  - Company: ABC Corp

## Important Notes

- **No request body needed** - just send the POST request
- **API key is required** - without it you'll get a 401 error
- **Processes all configured tables** - not just one specific table
- **Updates existing records** - doesn't create new tables
- **Idempotent** - safe to call multiple times

## **API vs Web Interface: What's the Difference?**

**This API Endpoint (`/api/trigger-dedupe`):**
- **Direct Airtable integration** - Updates your base immediately
- **No file downloads** - Data stays in Airtable
- **JSON response only** - Just success/failure confirmation
- **Perfect for automation** - Can be called from scripts, buttons, etc.

**Web Interface (other routes):**
- **CSV file uploads** - You upload files through the browser
- **CSV file downloads** - You get processed CSV files to download
- **HTML pages** - Interactive web forms and results
- **Manual process** - Good for one-time data cleaning

**Choose the API if you want:**
- Automated deduplication triggered by scripts
- Direct Airtable integration
- No file handling

**Choose the web interface if you want:**
- To process CSV files you have locally
- To download the results as CSV files
- Interactive control over the process

## Interactive API Documentation (Swagger)

For a complete interactive experience with your API, visit our **Swagger documentation**:

```
https://dedupee-app.vercel.app/docs
```

### What Swagger Gives You:
- **Interactive API testing** - try the API directly from the browser
- **Detailed parameter validation** - see exactly what's required
- **Response schemas** - understand all possible responses
- **Real-time testing** - test with your actual API key
- **Export options** - generate client code in multiple languages

### How to Use Swagger:
1. **Visit** `https://dedupee-app.vercel.app/docs`
2. **Click** on the `/api/trigger-dedupe` endpoint
3. **Click** "Try it out"
4. **Enter** your API key in the Authorization field
5. **Click** "Execute" to test the API
6. **See** the actual response in real-time

## Testing the API

### Option 1: Use Swagger (Recommended)
1. Go to `https://dedupee-app.vercel.app/docs`
2. Use the interactive interface to test
3. See results immediately

### Option 2: Use Code Examples
1. **Get your API key** from your system administrator
2. **Make a test call** using one of the examples above
3. **Check your Airtable** to see the updated records
4. **Review the response** to see processing statistics

## Troubleshooting

**Getting 401 errors?**
- Check that your API key is correct
- Make sure the Authorization header is included
- Verify the API key hasn't expired

**Getting 400 errors?**
- Check your Airtable configuration
- Verify your tables exist and are accessible
- Check the error message for specific details

**Getting 500 errors?**
- The deduplication process failed
- Check your Airtable connection
- Contact support if the issue persists

