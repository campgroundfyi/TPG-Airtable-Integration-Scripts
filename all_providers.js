
let destinationTable = base.getTable("All Providers");


let linkedRecordFields = [
    'Events',
    'Test Link',
    // Add any other fields that should preserve linked records
];

let idFields = [
    "UID",
    "NeonCRM Account ID", 
    "Circle Account ID (C)",
    "First Name",
    "Last Name", 
    "Email",
    "Registrant Full Name (F)",
    "Profile URL",
    "Tags",
    "Member Status",
    "Join Date",
    "Event RSVPs",
    "Event Attendance", 
    "Newsletter",
    "Program Applications",
    "Program Acceptances",
    "Engagement Score",
    "Events",
    // Add the column headers
];

let totalCreated = 0;
let processedTables = [];

for (let table of base.tables) {
    if (table.name === destinationTable.name || 
        table.name === "Events" || 
        table.name === "Deduplicated_Results") {
        continue; // Skip these tables
    }

    // Get actual field names for this table
    let tableFieldNames = table.fields.map(f => f.name);
    
    // Only use ID fields that actually exist in this table
    let fieldsToUse = idFields.filter(f => tableFieldNames.includes(f));
    
    let result = await table.selectRecordsAsync();
    let createdCount = 0;

    for (let record of result.records) {
        let newRecord = {
            "Provider Type": table.name
        };
        
        for (let field of fieldsToUse) {
            let cellValue = record.getCellValue(field);
            
            if (cellValue !== null && typeof cellValue !== "string") {
                if (Array.isArray(cellValue)) {
                    // Check if this looks like linked records (all items have 'id' property)
                    if (linkedRecordFields.includes(field) || 
                        cellValue.every(item => item && item.id && item.id.startsWith('rec'))) {
                        // Preserve linked records as array of objects with id property
                        newRecord[field] = cellValue.map(item => ({id: item.id}));
                    } else {
                        // Regular array - convert to comma-separated string
                        newRecord[field] = cellValue
                            .map(item => item.name || item.id || String(item))
                            .join(", ");
                    }
                } else if (typeof cellValue === "object") {
                    // Check if this is a single linked record
                    if (cellValue && cellValue.id && cellValue.id.startsWith('rec')) {
                        newRecord[field] = [{id: cellValue.id}]; // Single linked record as array of objects
                    } else {
                        newRecord[field] = cellValue.name || cellValue.id || JSON.stringify(cellValue);
                    }
                } else {
                    newRecord[field] = String(cellValue);
                }
            } else {
                newRecord[field] = cellValue;
            }
        }
        
        await destinationTable.createRecordAsync(newRecord);
        createdCount++;
        totalCreated++;
    }
    
    processedTables.push(table.name);
    output.markdown(`✅ Created ${createdCount} records from ${table.name}`);
}

output.markdown("**Data Consolidation Complete!**");
output.markdown(`\nProcessed ${processedTables.length} tables: ${processedTables.join(', ')}`);
output.markdown(`Created ${totalCreated} total records in All Providers table`);

// Trigger Python deduplication
output.markdown("**Triggering Python Deduplication...**");

try {
    const response = await fetch("https://dedupee-app.vercel.app/api/trigger-dedupe", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "X-API-Key": "HKzv196xkzEQLXHjY2o2rhCbTxiTLEo2zUyvqaEGjVA" // ← CORRECT API KEY
        }
    });
    
    const result = await response.json();
    
    if (response.ok && result.success) {
        output.markdown(`✅ Smart Deduplication Complete!\n\n` +
            `The Python backend has:\n` +
            `- Combined new data with existing All Providers data\n` +
            `- Run advanced deduplication algorithms\n` +
            `- Updated existing records (preserving comments/history)\n` +
            `- Created new records for unmatched data\n` +
            `- Generated unique TPG IDs for each deduplicated record\n\n` +
            `Your "All Providers" table now contains clean, deduplicated data!`);
    } else {
        output.markdown(`❌ Deduplication Failed: ${result.message || 'Unknown error'}`);
    }
} catch (error) {
    output.markdown(`❌ Error triggering deduplication: ${error.message}`);
}