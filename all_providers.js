// Airtable Script to consolidate data into "All Providers" table with linked record preservation

let destinationTable = base.getTable("All Providers"); //name of the new Table
let existingRecords = await destinationTable.selectRecordsAsync();

// Delete existing records in batches
for (let record of existingRecords.records) {
    await destinationTable.deleteRecordAsync(record.id);
}

output.markdown("**Cleared existing records from All Providers table**");

// Configuration for linked record fields
let linkedRecordFields = [
    'Events',  // Add your Events field here
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
   "TPG ID",
   "Member Status",
   "Join Date",
   "Event RSVPs",
   "Event Attendance",
   "Newsletter",
   "Program Applications",
   "Program Acceptances",
   "Engagement Score",
   "Events", 
]; //fields im searching for

for (let table of base.tables) {
   if (table.name === destinationTable.name || 
    table.name === "Events" ||
    table.name === "Deduplicated_Results"
    ) continue; //hide tables from the merging

   // Get actual field names for this table
   let tableFieldNames = table.fields.map(f => f.name);

   // Only use ID fields that actually exist in this table
   let fieldsToUse = idFields.filter(f => tableFieldNames.includes(f));

   let result = await table.selectRecordsAsync();

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
                       newRecord[field] = [{id: cellValue.id}];  // Single linked record as array of objects
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
   }
}
