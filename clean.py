import csv
import os
import statistics
# Define paths
SOURCE_DIR = r"e:\Techtatva\hackathon"
DATA_ENTRY_PATH = os.path.join(SOURCE_DIR, "Data_Entry.csv")
BBOX_LIST_PATH = os.path.join(SOURCE_DIR, "BBox_List.csv")
# Output paths
DATA_ENTRY_OUT = "Data_Entry_Cleaned.csv"
DATA_ENTRY_BAD_OUT = "Data_Entry_Bad.csv"
BBox_List_OUT = "BBox_List_Cleaned.csv"
BBox_List_BAD_OUT = "BBox_List_Bad.csv"
REPORT_OUT = "cleaning_report.md"
def clean_data():
    report_lines = []
    report_lines.append("# Data Cleaning Report\n")
    # --- Data_Entry.csv Cleaning ---
    print(f"Cleaning Data_Entry.csv from {DATA_ENTRY_PATH}...")
    
    cleaned_rows = []
    bad_rows = []
    initial_rows = 0
    headers = []
    ages = []
    
    try:
        with open(DATA_ENTRY_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Clean headers: strip spaces
            headers = [h.strip() for h in reader.fieldnames]
            
            # We need to read rows, but the DictReader keys are the original headers
            # We will map them to clean headers
            original_headers = reader.fieldnames
            header_map = {orig: orig.strip() for orig in original_headers}
            
            for original_row in reader:
                # Create a row with clean keys
                row = {header_map[k]: v for k, v in original_row.items() if k in header_map}
                
                initial_rows += 1
                
                # 1. Image Index Checking
                img_idx = row.get('Image Index', '')
                if 'invalid_image' in img_idx or 'missing_image' in img_idx:
                    row['Rejection_Reason'] = 'Invalid Image Index'
                    bad_rows.append(row)
                    continue
                
                # Standardize filename (keep original in bad row if we rejected it above, but here we process valid ones)
                if img_idx.startswith('IMG_'):
                    row['Image Index'] = img_idx.replace('IMG_', '')
                
                # 2. Gender Checking
                gender = row.get('Gender', '').strip()
                if gender in ['female', 'female ', 'F']:
                    row['Gender'] = 'F'
                elif gender in ['Male', 'M']:
                    row['Gender'] = 'M'
                else: 
                    # Drop unknown/missing
                    row['Rejection_Reason'] = f'Invalid/Missing Gender: {gender}'
                    bad_rows.append(row)
                    continue
                
                # 3. Patient Age
                age_str = str(row.get('patient_age', '')).upper().replace('Y', '').strip()
                try:
                    if age_str == 'TWENTY FIVE':
                        age = 25.0
                    else:
                        age = float(age_str)
                    
                    # Store age for median calc later, but keep invalid ones for now to filter?
                    # Actually, let's collect valid ages to calc median first?
                    # Two pass approach or buffering? 
                    # Buffering is fine for 100k rows.
                    
                    row['patient_age_clean'] = age # Temp storage
                    
                except ValueError:
                    row['patient_age_clean'] = None # Mark for filling
                
                # 4. Finding Labels
                garbage_labels = ['XYZ_Disease', '123', 'Unknown_Disorder']
                labels = row.get('finding_labels', '').split('|')
                clean_labels_list = [l.strip() for l in labels if l.strip() not in garbage_labels and l.strip() != 'None' and l.strip() != '']
                
                if not clean_labels_list:
                    row['finding_labels'] = "No Finding"
                else:
                    row['finding_labels'] = "|".join(clean_labels_list)
                
                cleaned_rows.append(row)
    except FileNotFoundError:
        print(f"Error: {DATA_ENTRY_PATH} not found.")
        return
    report_lines.append(f"## Data_Entry.csv\n")
    report_lines.append(f"- **Initial Rows**: {initial_rows}")
    
    # Age Processing: Median Replacement
    # Collect valid ages (0-120)
    valid_ages = [r['patient_age_clean'] for r in cleaned_rows if r['patient_age_clean'] is not None and 0 <= r['patient_age_clean'] <= 120]
    
    if valid_ages:
        median_age = int(round(statistics.median(valid_ages)))
    else:
        median_age = 0 # Fallback
        
    for row in cleaned_rows:
        age_val = row['patient_age_clean']
        if age_val is None or not (0 <= age_val <= 120):
            row['patient_age'] = str(median_age)
        else:
            row['patient_age'] = str(int(round(age_val)))
        
        # Remove temp key
        del row['patient_age_clean']
    report_lines.append(f"- **Rows after cleaning (Gender/Image)**: {len(cleaned_rows)}")
    report_lines.append(f"- **Age Cleaning**: Replaced outliers with median age ({median_age}).")
    # Deduplication
    # Convert dicts to unique set of tuples? 
    # Use a set of frozensets of items
    unique_rows = []
    seen = set()
    for row in cleaned_rows:
        # Create a tuple of items sorted by key to be hashable
        row_tuple = tuple(sorted(row.items()))
        if row_tuple not in seen:
            seen.add(row_tuple)
            unique_rows.append(row)
            
    report_lines.append(f"- **Duplicates Removed**: {len(cleaned_rows) - len(unique_rows)}")
    report_lines.append(f"- **Final Rows**: {len(unique_rows)}\n")
    # Write Data Entry
    print(f"Saving to {DATA_ENTRY_OUT}...")
    if unique_rows:
        with open(DATA_ENTRY_OUT, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(unique_rows)
    # Write Bad Data Entry
    print(f"Saving to {DATA_ENTRY_BAD_OUT}...")
    if bad_rows:
        # Add Rejection_Reason to headers for bad file
        bad_headers = headers + ['Rejection_Reason'] if headers else ['Rejection_Reason']
        with open(DATA_ENTRY_BAD_OUT, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=bad_headers)
            writer.writeheader()
            writer.writerows(bad_rows)
    # --- BBox_List.csv Cleaning ---
    print(f"Cleaning BBox_List.csv from {BBOX_LIST_PATH}...")
    cleaned_bbox_rows = []
    bad_bbox_rows = []
    initial_bbox_rows = 0
    bbox_headers = []
    
    try:
        with open(BBOX_LIST_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            bbox_headers = [h.strip() for h in reader.fieldnames] # Clean headers
            
            # Map old headers to new clean ones if needed? 
            # The reader keys will still be the original ones (with spaces).
            # We need to map them.
            
            original_headers = reader.fieldnames
            
            for row in reader:
                initial_bbox_rows += 1
                img_idx = row.get('Image Index', '')
                if 'invalid_image' in img_idx or 'missing_image' in img_idx:
                    row['Rejection_Reason'] = 'Invalid Image Index'
                    bad_bbox_rows.append(row)
                    continue
                
                # Check BBox coords
                # Keys: 'bbox_x ', 'bbox-y', 'width', 'height' (based on view_file output)
                # Let's try to get values safely
                
                try:
                    x = float(row.get('bbox_x ', row.get('bbox_x', 0))) # Try with space
                    y = float(row.get('bbox-y', row.get('bbox_y', 0)))
                    w = float(row.get('width', 0))
                    h = float(row.get('height', 0))
                    
                    if x >= 0 and y >= 0 and w > 0 and h > 0 and x < 5000 and y < 5000 and w < 5000 and h < 5000:
                        # Clean Headers in the row dict for output?
                        # Let's standarize output headers: Image Index, Finding Label, bbox_x, bbox_y, width, height
                        new_row = {
                            'Image Index': img_idx,
                            'Finding Label': row.get('Finding Label', ''),
                            'bbox_x': x,
                            'bbox_y': y,
                            'width': w,
                            'height': h
                        }
                        cleaned_bbox_rows.append(new_row)
                    else:
                        row['Rejection_Reason'] = f'Invalid Coordinates: x={x}, y={y}, w={w}, h={h}'
                        bad_bbox_rows.append(row)
                        
                except ValueError:
                    row['Rejection_Reason'] = 'Malformed Coordinates'
                    bad_bbox_rows.append(row)
                    continue # Drop rows with bad parsing
    except FileNotFoundError:
        print(f"Error: {BBOX_LIST_PATH} not found.")
        report_lines.append(f"\nError: BBox_List.csv not found.")
        
    report_lines.append(f"## BBox_List.csv\n")
    report_lines.append(f"- **Initial Rows**: {initial_bbox_rows}")
    report_lines.append(f"- **Final Rows**: {len(cleaned_bbox_rows)}")
    
    # Write BBox List
    print(f"Saving to {BBox_List_OUT}...")
    output_bbox_headers = ['Image Index', 'Finding Label', 'bbox_x', 'bbox_y', 'width', 'height']
    if cleaned_bbox_rows:
        with open(BBox_List_OUT, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_bbox_headers)
            writer.writeheader()
            writer.writerows(cleaned_bbox_rows)
    # Write Bad BBox List
    print(f"Saving to {BBox_List_BAD_OUT}...")
    if bad_bbox_rows:
        # Use original headers + reason
        bad_bbox_headers = bbox_headers + ['Rejection_Reason'] if bbox_headers else ['Rejection_Reason']
        # We need to map keys back if we want to write full row?
        # The bad_bbox_rows contain original keys because we appended 'row' from reader
        # But 'row' from DictReader might have spaces in keys.
        # We collected 'bbox_headers' cleaned earlier, let's use the keys present in the dict.
        if bad_bbox_rows:
             sample_keys = list(bad_bbox_rows[0].keys())
             with open(BBox_List_BAD_OUT, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sample_keys)
                writer.writeheader()
                writer.writerows(bad_bbox_rows)
    # Write Report
    with open(REPORT_OUT, 'w') as f:
        f.writelines(report_lines)
    
    print("Cleaning complete.")
if __name__ == "__main__":
    clean_data()
