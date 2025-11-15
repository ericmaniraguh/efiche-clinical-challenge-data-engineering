#!/usr/bin/env python3
"""
PadChest Incremental Data Generator - FINAL VERSION (DateOfBirth Instead of Age)
- Generates DateOfBirth (YYYY-MM-DD) instead of random PatientAge
- Age will be CALCULATED in database, not stored
- Consistent DOB across patient encounters
- Reads all config from .env using os.getenv()
- Has default values for all variables
- Prints generated columns list
"""

import pandas as pd
import numpy as np
import random
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
import os

# ============================================================================
# STEP 1: LOAD .ENV FILE FIRST
# ============================================================================

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ .env file loaded successfully\n")
except ImportError:
    print("⚠ Warning: python-dotenv not installed. Using default values.\n")

# ============================================================================
# STEP 2: READ CONFIGURATION FROM .ENV (with defaults)
# ============================================================================

NUM_ROWS = int(os.getenv('NUM_ROWS', '1000'))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "../data/padchest_synthetic_data.csv")
STATE_FILE = os.getenv('STATE_FILE', '../data/padchest_synthetic_data.json')
APPEND_MODE = os.getenv('APPEND_MODE', 'true').lower() == 'true'

# Normalize paths to relative paths
OUTPUT_FILE = OUTPUT_FILE.lstrip('/').replace('\\', '/')
STATE_FILE = STATE_FILE.lstrip('/').replace('\\', '/')

print("="*70)
print("CONFIGURATION LOADED FROM .ENV")
print("="*70)
print(f"NUM_ROWS:        {NUM_ROWS:,}")
print(f"OUTPUT_FILE:     {OUTPUT_FILE}")
print(f"STATE_FILE:      {STATE_FILE}")
print(f"APPEND_MODE:     {APPEND_MODE}")
print("="*70 + "\n")

# Default values for data generation
DEFAULT_CONFIG = {
    'diagnoses': [
        'normal', 'pneumonia', 'edema', 'cardiomegaly', 'effusion',
        'atelectasis', 'consolidation', 'pneumothorax', 'nodule',
        'mass', 'emphysema', 'fibrosis', 'pleural_thickening'
    ],
    'modalities': ['DX', 'CR', 'RF', 'DR'],
    'projections': ['PA', 'AP', 'LATERAL', 'OBLIQUE'],
    'locations': ['Rwanda', 'USA', 'Spain', 'Germany', 'UK'],
    'institutions': ['General Hospital', 'County Medical', 'Central Clinic', 'Teaching Hospital']
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('padchest_incremental')

# ============================================================================
# PATIENT REGISTRY - For Consistent DOB Across Encounters
# ============================================================================

class PatientRegistry:
    """
    Maintains consistent patient DOBs across multiple encounters
    Ensures same patient (PAT001) always has the SAME DateOfBirth
    """
    
    def __init__(self):
        self.patients = {}  # {patient_id: {'dob': date, 'birth_year': int}}
    
    def get_or_create_patient(self, patient_id, birth_year_range=(1935, 2005)):
        """
        Get existing patient or create new one with consistent DOB
        
        Args:
            patient_id: Patient identifier (e.g., 'PAT000001')
            birth_year_range: Tuple of (min_year, max_year) for birth year
        
        Returns:
            date object (date_of_birth)
        """
        
        if patient_id in self.patients:
            # Return existing patient DOB
            return self.patients[patient_id]['dob']
        
        # Create new patient with consistent DOB
        birth_year = random.randint(birth_year_range[0], birth_year_range[1])
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Use 28 to avoid Feb 29 issues
        
        dob = datetime(birth_year, birth_month, birth_day).date()
        
        self.patients[patient_id] = {
            'dob': dob,
            'birth_year': birth_year
        }
        
        return dob
    
    def get_stats(self):
        """Get statistics about registered patients"""
        if not self.patients:
            return {'total_patients': 0}
        
        birth_years = [p['birth_year'] for p in self.patients.values()]
        
        return {
            'total_patients': len(self.patients),
            'min_birth_year': min(birth_years),
            'max_birth_year': max(birth_years),
            'birth_year_range': f"{min(birth_years)} - {max(birth_years)}"
        }


# ============================================================================
# DATA MANAGER CLASS
# ============================================================================

class IncrementalDataManager:
    """Manages incremental data loading with state tracking."""
    
    def __init__(self, output_file, state_file=STATE_FILE):
        """Initialize the data manager."""
        self.output_file = Path(output_file)
        self.state_file = Path(state_file)
        self.state = self._load_state()
        logger.info(f"Initialized DataManager")
        logger.info(f"  Output: {self.output_file}")
        logger.info(f"  State: {self.state_file}")
    
    def _load_state(self):
        """Load existing state or create new with all required fields."""
        default_state = {
            'total_rows': 0,
            'num_runs': 0,
            'created_at': datetime.now().isoformat(),
            'last_run': None,
            'runs': []
        }
        
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    loaded_state = json.load(f)
                # Merge with defaults to ensure all keys exist
                merged_state = default_state.copy()
                merged_state.update(loaded_state)
                logger.info(f"Loaded state from {self.state_file}")
                return merged_state
            except Exception as e:
                logger.warning(f"Error loading state: {e}, creating new state")
        
        return default_state
    
    def _save_state(self):
        """Save current state to JSON file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)
        logger.info(f"State saved: {self.state_file}")
    
    def append_data(self, new_df, description=""):
        """Append new data to existing dataset."""
        logger.info(f"Appending {len(new_df):,} rows...")
        
        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        result = {
            'action': 'append',
            'timestamp': datetime.now().isoformat(),
            'rows_added': len(new_df),
            'description': description
        }
        
        if self.output_file.exists():
            # File exists - append to it
            existing_df = pd.read_csv(self.output_file)
            previous_rows = len(existing_df)
            
            # Concatenate
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates if any
            if 'ImageID' in combined_df.columns:
                duplicates_before = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=['ImageID'], keep='first')
                duplicates_removed = duplicates_before - len(combined_df)
                if duplicates_removed > 0:
                    logger.warning(f"Removed {duplicates_removed} duplicate ImageIDs")
                    result['duplicates_removed'] = duplicates_removed
            
            # Save
            combined_df.to_csv(self.output_file, index=False)
            total_rows = len(combined_df)
            
            print(f"\n{'='*70}")
            print("✓ DATA APPENDED SUCCESSFULLY")
            print(f"{'='*70}")
            print(f"Previous rows:     {previous_rows:>15,}")
            print(f"New rows added:    {len(new_df):>15,}")
            print(f"Total rows now:    {total_rows:>15,}")
            print(f"File size:         {self.output_file.stat().st_size / (1024*1024):>15.2f} MB")
            print(f"{'='*70}\n")
            
            result['previous_rows'] = previous_rows
            result['total_rows'] = total_rows
            result['status'] = 'appended'
            
        else:
            # File doesn't exist - create it
            new_df.to_csv(self.output_file, index=False)
            total_rows = len(new_df)
            
            print(f"\n{'='*70}")
            print("✓ NEW FILE CREATED")
            print(f"{'='*70}")
            print(f"Initial rows:      {total_rows:>15,}")
            print(f"File size:         {self.output_file.stat().st_size / (1024*1024):>15.2f} MB")
            print(f"{'='*70}\n")
            
            result['total_rows'] = total_rows
            result['status'] = 'created'
        
        # Update state with all required fields
        self.state['total_rows'] = len(pd.read_csv(self.output_file))
        self.state['num_runs'] += 1
        self.state['last_run'] = datetime.now().isoformat()
        self.state['runs'].append(result)
        self._save_state()
        
        return result
    
    def replace_data(self, new_df, description=""):
        """Replace entire dataset."""
        logger.warning("REPLACING DATA - OLD DATA WILL BE LOST!")
        
        result = {
            'action': 'replace',
            'timestamp': datetime.now().isoformat(),
            'rows_new': len(new_df),
            'description': description
        }
        
        if self.output_file.exists():
            old_df = pd.read_csv(self.output_file)
            result['rows_old'] = len(old_df)
            
            print(f"\n{'='*70}")
            print("⚠ DATA REPLACEMENT IN PROGRESS")
            print(f"{'='*70}")
            print(f"Previous rows:     {len(old_df):>15,}")
            print(f"New rows:          {len(new_df):>15,}")
        else:
            print(f"\n{'='*70}")
            print("✓ CREATING NEW FILE")
            print(f"{'='*70}")
        
        # Save new data
        new_df.to_csv(self.output_file, index=False)
        
        print(f"Total rows:        {len(new_df):>15,}")
        print(f"File size:         {self.output_file.stat().st_size / (1024*1024):>15.2f} MB")
        print(f"{'='*70}\n")
        
        result['total_rows'] = len(new_df)
        result['status'] = 'replaced'
        
        # Update state properly
        self.state['total_rows'] = len(new_df)
        self.state['num_runs'] += 1
        self.state['last_run'] = datetime.now().isoformat()
        self.state['runs'].append(result)
        self._save_state()
        
        return result
    
    def reset(self, confirm=True):
        """Delete all data and reset state."""
        if confirm:
            response = input("⚠ This will DELETE all data. Continue? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Reset cancelled")
                return
        
        if self.output_file.exists():
            self.output_file.unlink()
            logger.info(f"Deleted: {self.output_file}")
        
        if self.state_file.exists():
            self.state_file.unlink()
            logger.info(f"Deleted: {self.state_file}")
        
        self.state = self._load_state()
        print("✓ Data reset complete - ready for fresh start\n")
    
    def status(self):
        """Print current data status."""
        if not self.output_file.exists():
            print("\nℹ No data file yet. Run the generator to create one.\n")
            return
        
        df = pd.read_csv(self.output_file)
        size_mb = self.output_file.stat().st_size / (1024 * 1024)
        
        print(f"\n{'='*70}")
        print(" DATA STATUS")
        print(f"{'='*70}")
        print(f"File:              {self.output_file}")
        print(f"Total rows:        {len(df):>15,}")
        print(f"Total columns:     {len(df.columns):>15}")
        print(f"File size:         {size_mb:>15.2f} MB")
        print(f"Number of runs:    {self.state['num_runs']:>15}")
        print(f"Created:           {self.state['created_at']}")
        last_run = self.state.get('last_run') or 'Never'
        print(f"Last updated:      {last_run}")
        print(f"{'='*70}\n")
    
    def print_columns(self, df):
        """Print all columns in the DataFrame."""
        print(f"\n{'='*70}")
        print("COLUMNS IN GENERATED DATASET")
        print(f"{'='*70}\n")
        
        columns = df.columns.tolist()
        for i, col in enumerate(columns, 1):
            print(f"{i:2d}. {col}")
        
        print(f"\n{'='*70}")
        print(f"Total: {len(columns)} columns")
        print(f"{'='*70}\n")
    
    def print_runs_history(self):
        """Print history of all runs."""
        if not self.state['runs']:
            print("\nNo runs yet.\n")
            return
        
        print(f"\n{'='*70}")
        print("RUNS HISTORY")
        print(f"{'='*70}")
        
        for i, run in enumerate(self.state['runs'], 1):
            print(f"\nRun {i}:")
            print(f"  Action: {run['action']}")
            print(f"  Rows: {run.get('rows_added', run.get('rows_new', '?')):,}")
            print(f"  Time: {run['timestamp']}")
            if run.get('description'):
                print(f"  Note: {run['description']}")
        
        print(f"\n{'='*70}\n")

# ============================================================================
# DATA GENERATION WITH DATEOFBIRTH
# ============================================================================

def generate_padchest_data(num_rows, config=None, append_mode=None, csv_file=None, state_file=None):
    """
    Generate synthetic PadChest-like data with DateOfBirth instead of Age
    
    Key: Age will be CALCULATED in database, not stored in CSV
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    logger.info(f"Generating {num_rows:,} synthetic records with DateOfBirth...")
    
    diagnoses = config['diagnoses']
    modalities = config['modalities']
    projections = config['projections']
    locations = config['locations']
    institutions = config['institutions']
    
    # =====================================================================
    # STEP 1: Create patient registry with consistent DOBs
    # =====================================================================
    patient_registry = PatientRegistry()
    
    # Generate patient IDs (allow repeats for multiple encounters)
    # About 85% unique, 15% repeat for multiple visits
    unique_patients = max(1, int(num_rows * 0.85))
    patient_ids = []
    
    for i in range(num_rows):
        if random.random() < 0.15 and patient_ids:  # 15% chance of repeat
            # Pick existing patient (multiple encounters)
            patient_id = random.choice(patient_ids)
        else:
            # Create new patient
            patient_id = f'PAT{i // max(1, int(num_rows / 1000)):06d}'
        
        patient_ids.append(patient_id)
    
    # =====================================================================
    # STEP 2: Generate study dates (past 2 years)
    # =====================================================================
    study_dates = []
    for _ in range(num_rows):
        days_ago = random.randint(1, 730)
        study_date = datetime.now() - timedelta(days=days_ago)
        study_dates.append(study_date)
    
    # =====================================================================
    # STEP 3: Generate labels
    # =====================================================================
    labels_list = []
    for _ in range(num_rows):
        k = random.randint(1, min(3, len(diagnoses)))
        label = '|'.join(random.sample(diagnoses, k=k))
        labels_list.append(label)

    def generate_image_id():
        folder = random.randint(1, 50)
        hash_id = ''.join([str(random.randint(0, 9)) for _ in range(36)])
        study = random.randint(0, 99)
        series = random.randint(0, 999)
        image = random.randint(0, 999)
        return f"{folder}/{hash_id}_{study:02d}-{series:03d}-{image:03d}.png"

    # =====================================================================
    # STEP 4: Generate data with DateOfBirth (NO age column)
    # =====================================================================
    
    date_of_births = []
    
    for i in range(num_rows):
        patient_id = patient_ids[i]
        
        # Get consistent DOB for this patient
        dob = patient_registry.get_or_create_patient(patient_id)
        date_of_births.append(dob)
    
    # Build dataset WITHOUT age column
    data = {
        'ImageID': [generate_image_id() for _ in range(num_rows)],
        'PatientID': patient_ids,
        'DateOfBirth': date_of_births,  # ✓ NEW: DateOfBirth instead of Age
        'StudyID': [f'STD{random.randint(100000, 999999)}' for _ in range(num_rows)],
        'PatientSex': np.random.choice(['M', 'F'], num_rows),
        'PatientHeight': np.random.randint(150, 210, num_rows),
        'PatientWeight': np.random.randint(40, 150, num_rows),
        'StudyDate': study_dates,
        'Projection': np.random.choice(projections, num_rows),
        'Modality': np.random.choice(modalities, num_rows),
        'Location': np.random.choice(locations, num_rows),
        'InstitutionName': np.random.choice(institutions, num_rows),
        'Labels': labels_list,
        'Report': [f'Chest radiograph shows {random.choice(diagnoses)} findings.' for _ in range(num_rows)],
        'Findings': [f'{random.choice(diagnoses).upper()}: patient presents with respiratory symptoms' for _ in range(num_rows)],
        'Impression': [f'Findings consistent with {random.choice(diagnoses)}' for _ in range(num_rows)],
        'CreatedAt': [datetime.now() for _ in range(num_rows)],
    }
    
    df = pd.DataFrame(data)
    
    # Convert dates to string for CSV
    df['DateOfBirth'] = pd.to_datetime(df['DateOfBirth']).dt.strftime('%Y-%m-%d')
    df['StudyDate'] = pd.to_datetime(df['StudyDate']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['CreatedAt'] = pd.to_datetime(df['CreatedAt']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Generated {len(df):,} records with {len(df.columns)} columns")
    
    # Print patient statistics
    patient_stats = patient_registry.get_stats()
    print(f"\n{'='*70}")
    print("PATIENT REGISTRY STATISTICS")
    print(f"{'='*70}")
    print(f"Total unique patients:  {patient_stats['total_patients']:>10,}")
    print(f"Birth year range:       {patient_stats['birth_year_range']:>10}")
    print(f"{'='*70}\n")

    # If called with file/state parameters, save the generated data and
    # return a dict compatible with the Airflow DAG expectations.
    if (append_mode is not None) or (csv_file is not None) or (state_file is not None):
        out_file = csv_file or OUTPUT_FILE
        st_file = state_file or STATE_FILE
        use_append = append_mode if append_mode is not None else APPEND_MODE

        manager = IncrementalDataManager(out_file, st_file)

        if use_append:
            res = manager.append_data(df, description=f"Added {num_rows:,} rows (generator wrapper)")
        else:
            res = manager.replace_data(df, description=f"Replaced with {num_rows:,} rows (generator wrapper)")

        return {
            'status': 'success',
            'rows_generated': len(df),
            'total_rows': res.get('total_rows', len(df))
        }

    return df, patient_registry

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution function."""
    print(f"\n{'='*70}")
    print("PADCHEST INCREMENTAL DATA GENERATOR (DateOfBirth Version)")
    print(f"{'='*70}")
    print(f"Mode:              {'APPEND' if APPEND_MODE else 'REPLACE'}")
    print(f"Number of rows:    {NUM_ROWS:,}")
    print(f"Output file:       {OUTPUT_FILE}")
    print(f"State file:        {STATE_FILE}")
    print(f"Generator Type:    DateOfBirth (Age calculated in DB)")
    print(f"{'='*70}\n")
    
    try:
        # Initialize manager
        manager = IncrementalDataManager(OUTPUT_FILE, STATE_FILE)
        
        # Show current status
        print("BEFORE:")
        manager.status()
        
        # Generate new data with DateOfBirth
        new_df, patient_registry = generate_padchest_data(NUM_ROWS)
        
        # Print columns generated
        manager.print_columns(new_df)
        
        # Show sample
        print("Generated data sample (first 5 rows):")
        sample_cols = ['ImageID', 'PatientID', 'DateOfBirth', 'StudyDate', 'PatientSex', 'PatientHeight']
        print(new_df[sample_cols].head(5).to_string(index=False))
        print()
        
        # Show patient consistency example
        print(f"\n{'='*70}")
        print("SAMPLE: Patient with consistent DateOfBirth")
        print(f"{'='*70}")
        
        patient_counts = new_df['PatientID'].value_counts()
        repeat_patients = patient_counts[patient_counts > 1]
        
        if len(repeat_patients) > 0:
            sample_patient = repeat_patients.index[0]
            patient_data = new_df[new_df['PatientID'] == sample_patient][
                ['PatientID', 'DateOfBirth', 'StudyDate']
            ]
            print(f"\nPatient {sample_patient} appears {repeat_patients[sample_patient]} times:")
            print(patient_data.to_string(index=False))
            print("\n✓ Notice: Same DateOfBirth across all encounters!")
        else:
            print("\nℹ No patients with multiple encounters in this batch")
        
        print(f"{'='*70}\n")
        
        # Save (append or replace)
        if APPEND_MODE:
            result = manager.append_data(new_df, description=f"Added {NUM_ROWS:,} rows with DateOfBirth")
        else:
            result = manager.replace_data(new_df, description=f"Replaced with {NUM_ROWS:,} rows with DateOfBirth")
        
        # Show new status
        print("AFTER:")
        manager.status()
        
        # Show runs history
        manager.print_runs_history()
        
        # Key information
        print(f"\n{'='*70}")
        print("KEY INFORMATION")
        print(f"{'='*70}")
        print("✓ Column removed: PatientAge (random values)")
        print("✓ Column added: DateOfBirth (consistent across encounters)")
        print("✓ Age will be calculated in database using:")
        print("  EXTRACT(YEAR FROM AGE(NOW(), date_of_birth))::INT")
        print("✓ Age at study will be calculated using:")
        print("  EXTRACT(YEAR FROM AGE(study_date, date_of_birth))::INT")
        print(f"{'='*70}\n")
        
        logger.info("✓ Data generation completed successfully!")
        print("✓ Data generation completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        print(f"\n ERROR: {str(e)}\n")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())


# ---------------------------------------------------------------------------
# Backwards-compatible wrapper expected by the Airflow DAG
# ---------------------------------------------------------------------------
def generate_synthetic_data(num_rows=None, append=True, csv_file=None, state_file=None):
    """Compatibility wrapper so DAG can call the generator.

    Args:
        num_rows (int): number of rows to generate
        append (bool): whether to append (True) or replace (False)
        csv_file (str): path to output CSV
        state_file (str): path to state JSON

    Returns:
        dict: {'status': 'success'|'failed', 'rows_generated': int, 'total_rows': int, 'error': str (opt)}
    """
    try:
        rows = int(num_rows) if num_rows is not None else NUM_ROWS
        out_file = csv_file or OUTPUT_FILE
        st_file = state_file or STATE_FILE

        # Generate dataframe
        new_df, _ = generate_padchest_data(rows)

        manager = IncrementalDataManager(out_file, st_file)

        if append:
            res = manager.append_data(new_df, description=f"Added {rows:,} rows via DAG wrapper")
        else:
            res = manager.replace_data(new_df, description=f"Replaced with {rows:,} rows via DAG wrapper")

        return {
            'status': 'success',
            'rows_generated': rows,
            'total_rows': res.get('total_rows', len(new_df))
        }

    except Exception as e:
        logger.error(f"Wrapper generation failed: {e}", exc_info=True)
        return {'status': 'failed', 'rows_generated': 0, 'total_rows': 0, 'error': str(e)}
