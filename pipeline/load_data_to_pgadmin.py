"""
eFICHE Clinical Data ETL Pipeline - Load to PostgreSQL
Production-ready module for Airflow integration
Handles FK constraints with proper commit strategy
"""

import os
import sys
import re
import logging
import pandas as pd
import psycopg2
import uuid
import random
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

# =============================================================================
# ENVIRONMENT & LOGGING
# =============================================================================
load_dotenv()

def safe_int_env(key: str, default: int) -> int:
    """Safely parse integer from environment variable"""
    value = os.getenv(key, str(default))
    if not value:
        return default
    match = re.match(r'^(\d+)', str(value).strip())
    if match:
        return int(match.group(1))
    return default

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = safe_int_env('DB_PORT', 543)
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME = os.getenv('DB_NAME', 'efiche_clinical_database')

# CSV file path resolution: resolve relative paths from project root (one level up from pipeline/)
_csv_default = 'data/padchest_synthetic_data.csv'
_csv_from_env = os.getenv("ETL_CSV_FILE", _csv_default)
if not os.path.isabs(_csv_from_env):
    # Relative path: resolve from project root
    _pipeline_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.dirname(_pipeline_dir)
    ETL_CSV_FILE = os.path.join(_project_root, _csv_from_env)
else:
    # Absolute path: use as-is
    ETL_CSV_FILE = _csv_from_env

BATCH_SIZE = safe_int_env("BATCH_SIZE", 100)
LOG_DIR = os.getenv('LOG_DIR', './logs')
LOG_FILE = os.getenv('LOG_FILE', 'padchest_etl.log')

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =============================================================================
# MASTER DATA MAPS
# =============================================================================
DIAGNOSIS_CATEGORY_MAP = {
    'normal': 'Normal',
    'pneumonia': 'Infectious',
    'edema': 'Pulmonary',
    'cardiomegaly': 'Cardiac',
    'effusion': 'Pleural',
    'atelectasis': 'Pulmonary',
    'consolidation': 'Pulmonary',
    'pneumothorax': 'Pleural',
    'nodule': 'Mass',
    'mass': 'Mass',
    'emphysema': 'Chronic Pulmonary',
    'fibrosis': 'Chronic Pulmonary',
    'pleural_thickening': 'Pleural',
}

PROJECTION_METADATA = {
    'PA': {'name': 'Posteroanterior', 'description': 'Frontal PA chest projection'},
    'AP': {'name': 'Anteroposterior', 'description': 'Frontal AP projection'},
    'LATERAL': {'name': 'Lateral', 'description': 'Side lateral projection'},
    'OBLIQUE': {'name': 'Oblique', 'description': 'Oblique angle projection'}
}

DEFAULT_REGION = {'code': 'thorax', 'name': 'Thoracic Cavity', 'body_system': 'Respiratory'}
REGION_KEYWORDS = [
    ('cardio', {'code': 'cardiac', 'name': 'Cardiac Region', 'body_system': 'Cardiovascular'}),
    ('pleur', {'code': 'pleural', 'name': 'Pleural Space', 'body_system': 'Respiratory'}),
    ('lung', {'code': 'lung', 'name': 'Lung Parenchyma', 'body_system': 'Respiratory'}),
    ('nodule', {'code': 'pulmonary_mass', 'name': 'Pulmonary Mass Region', 'body_system': 'Respiratory'}),
    ('mass', {'code': 'pulmonary_mass', 'name': 'Pulmonary Mass Region', 'body_system': 'Respiratory'}),
    ('fibrosis', {'code': 'fibrotic', 'name': 'Fibrotic Tissue', 'body_system': 'Respiratory'}),
]

TECHNICIAN_NAMES = [
    'Alice M.', 'Brian K.', 'Charlotte L.', 'David P.', 'Evelyn R.', 'Felix O.',
    'Grace T.', 'Hector V.', 'Isabella Q.', 'Jonas H.'
]
PHYSICIAN_NAMES = [
    'Amazi', 'Kamali', 'Murenzi', 'Niyonsaba', 'Uwase', 'Habimana', 'Nsengimana',
    'Gatera', 'Mukamana', 'Iradukunda'
]
REVIEWER_NAMES = [
    'Smith', 'Lopez', 'Ngugi', 'Kamanzi', 'Patel', 'Fischer', 'Hernandez'
]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def infer_diagnosis_category(name: str) -> str:
    if not name:
        return 'Unspecified'
    return DIAGNOSIS_CATEGORY_MAP.get(name.lower().strip(), 'Other')

def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value
    try:
        dt = pd.to_datetime(value)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None

def parse_date(value: Any) -> Optional[date]:
    dt = parse_datetime(value)
    return dt.date() if dt else None

def safe_int_value(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(float(value))
    except Exception:
        return None

def infer_region(labels_text: str) -> Dict[str, str]:
    if labels_text:
        lower = labels_text.lower()
        for keyword, region in REGION_KEYWORDS:
            if keyword in lower:
                return region
    return DEFAULT_REGION

def get_random_name(names: List[str], prefix: str = '') -> str:
    if not names:
        return prefix.strip()
    chosen = random.choice(names)
    return f"{prefix}{chosen}".strip()

def summarize_text(text: str, max_chars: int = 500) -> str:
    if not text:
        return ""
    clean = str(text).strip()
    return clean if len(clean) <= max_chars else f"{clean[:max_chars-3]}..."

def derive_finding_severity(labels: str) -> str:
    if not labels or labels.lower().strip() in ('normal', ''):
        return 'Normal'
    label_count = len([l for l in labels.split('|') if l.strip()])
    if label_count == 1:
        return 'Mild'
    if label_count == 2:
        return 'Moderate'
    if label_count == 3:
        return 'Severe'
    return 'Critical'

def normalize_text(value: Any, default: str = '') -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return default
    return text

# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """Manages PostgreSQL connections with transaction handling"""
    
    def __init__(self, host, port, user, password, database):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.cursor = None
    
    def connect(self):
        try:
            logger.info(f"Connecting to {self.config['database']} at {self.config['host']}:{self.config['port']}")
            self.conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                connect_timeout=10
            )
            self.cursor = self.conn.cursor()
            logger.info("[OK] Database connected")
            return True
        except Exception as e:
            logger.error(f"[FAIL] Connection failed: {e}")
            return False
    
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("[OK] Database closed")
        except:
            pass
    
    def commit(self):
        try:
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Commit failed: {e}")
            self.rollback()
            return False
    
    def rollback(self):
        try:
            self.conn.rollback()
            logger.debug("Transaction rolled back")
        except:
            pass
    
    def execute(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def fetch_one(self, query, params=None):
        try:
            self.execute(query, params)
            return self.cursor.fetchone()
        except:
            return None

# =============================================================================
# MASTER DATA MANAGER
# =============================================================================

class MasterDataManager:
    """Manages master data with FK-safe commits after each insert"""
    
    def __init__(self, db):
        self.db = db
        self.facility_cache = {}
        self.modality_cache = {}
        self.diagnosis_cache = {}
        self.projection_cache = {}
        self.region_cache = {}
    
    def get_facility(self, name: str, location: str) -> str:
        key = f"fac:{name}"
        if key in self.facility_cache:
            return self.facility_cache[key]
        
        try:
            code = name.replace(' ', '_').lower()[:50]
            result = self.db.fetch_one(
                "SELECT facility_id FROM master.facility_master WHERE facility_code = %s",
                (code,)
            )
            
            if result:
                fid = result[0]
            else:
                fid = str(uuid.uuid4())
                self.db.execute(
                    "INSERT INTO master.facility_master (facility_id, facility_code, facility_name, location) VALUES (%s, %s, %s, %s)",
                    (fid, code, name, location)
                )
                self.db.commit()  # CRITICAL: Commit for FK
            
            self.facility_cache[key] = fid
            return fid
        except Exception as e:
            logger.error(f"get_facility failed: {e}")
            self.db.rollback()
            raise
    
    def get_modality(self, code: str) -> str:
        key = f"mod:{code}"
        if key in self.modality_cache:
            return self.modality_cache[key]
        
        try:
            mnames = {'DX': 'Digital', 'CR': 'Computed', 'RF': 'Radio', 'DR': 'Digital'}
            name = mnames.get(code, f'Modality {code}')
            
            result = self.db.fetch_one(
                "SELECT modality_id FROM master.modality_master WHERE modality_code = %s",
                (code,)
            )
            
            if result:
                mid = result[0]
            else:
                mid = str(uuid.uuid4())
                self.db.execute(
                    "INSERT INTO master.modality_master (modality_id, modality_code, modality_name) VALUES (%s, %s, %s)",
                    (mid, code, name)
                )
                self.db.commit()  # CRITICAL: Commit for FK
            
            self.modality_cache[key] = mid
            return mid
        except Exception as e:
            logger.error(f"get_modality failed: {e}")
            self.db.rollback()
            raise
    
    def get_projection(self, code: str) -> str:
        norm_code = (code or 'UNKNOWN').strip().upper() or 'UNKNOWN'
        key = f"proj:{norm_code}"
        if key in self.projection_cache:
            return self.projection_cache[key]
        
        try:
            result = self.db.fetch_one(
                "SELECT projection_id FROM master.projection_master WHERE projection_code = %s",
                (norm_code,)
            )
            if result:
                pid = result[0]
            else:
                pid = str(uuid.uuid4())
                meta = PROJECTION_METADATA.get(norm_code, {})
                name = meta.get('name', norm_code.title())
                desc = meta.get('description', f"{norm_code} projection")
                self.db.execute(
                    "INSERT INTO master.projection_master (projection_id, projection_code, projection_name, description) VALUES (%s, %s, %s, %s)",
                    (pid, norm_code, name, desc)
                )
                self.db.commit()
            self.projection_cache[key] = pid
            return pid
        except Exception as e:
            logger.error(f"get_projection failed: {e}")
            self.db.rollback()
            raise
    
    def get_region(self, code: str, name: str, body_system: str) -> str:
        norm_code = (code or DEFAULT_REGION['code']).strip().lower()[:50]
        key = f"region:{norm_code}"
        if key in self.region_cache:
            return self.region_cache[key]
        
        region_name = name or DEFAULT_REGION['name']
        body_sys = body_system or DEFAULT_REGION['body_system']
        
        try:
            result = self.db.fetch_one(
                "SELECT region_id FROM master.anatomical_region_master WHERE region_code = %s",
                (norm_code,)
            )
            if result:
                rid = result[0]
            else:
                rid = str(uuid.uuid4())
                self.db.execute(
                    "INSERT INTO master.anatomical_region_master (region_id, region_code, region_name, body_system) VALUES (%s, %s, %s, %s)",
                    (rid, norm_code, region_name, body_sys)
                )
                self.db.commit()
            self.region_cache[key] = rid
            return rid
        except Exception as e:
            logger.error(f"get_region failed: {e}")
            self.db.rollback()
            raise
    
    def get_diagnosis(self, name: str) -> str:
        key = f"diag:{name}"
        if key in self.diagnosis_cache:
            return self.diagnosis_cache[key]
        
        try:
            code = name.replace(' ', '_').lower()[:50]
            category = infer_diagnosis_category(name)
            result = self.db.fetch_one(
                "SELECT diagnosis_id, category FROM master.diagnosis_master WHERE diagnosis_code = %s",
                (code,)
            )
            
            if result:
                did, existing_category = result
                if not existing_category and category:
                    self.db.execute(
                        "UPDATE master.diagnosis_master SET category = %s WHERE diagnosis_id = %s",
                        (category, did)
                    )
                    self.db.commit()
            else:
                did = str(uuid.uuid4())
                self.db.execute(
                    "INSERT INTO master.diagnosis_master (diagnosis_id, diagnosis_code, diagnosis_name, category) VALUES (%s, %s, %s, %s)",
                    (did, code, name, category)
                )
                self.db.commit()  # CRITICAL: Commit for FK
            
            self.diagnosis_cache[key] = did
            return did
        except Exception as e:
            logger.error(f"get_diagnosis failed: {e}")
            self.db.rollback()
            raise

# =============================================================================
# ETL PIPELINE
# =============================================================================

class ETLPipeline:
    def __init__(self, csv_file: str, db: DatabaseManager):
        self.csv_file = csv_file
        self.db = db
        self.master = MasterDataManager(db)
        self.stats = {'total': 0, 'success': 0, 'failed': 0, 'errors': []}
        self.encounter_code_tracker: Dict[str, int] = {}
    
    def load_csv(self) -> pd.DataFrame:
        try:
            logger.info(f"Loading CSV: {self.csv_file}")
            csv_path = Path(self.csv_file)
            if not csv_path.is_absolute():
                csv_path = Path.cwd() / csv_path
            
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV not found: {csv_path}")
            
            df = pd.read_csv(str(csv_path))
            logger.info(f"[OK] Loaded {len(df):,} rows")
            return df
        except Exception as e:
            logger.error(f"[FAIL] CSV load failed: {e}")
            raise

    def _generate_encounter_code(self, base_code: str) -> str:
        normalized = (base_code or 'ENC').strip().replace(' ', '_') or 'ENC'
        normalized = normalized[:45]
        count = self.encounter_code_tracker.get(normalized, 0)
        if count == 0:
            code = normalized
        else:
            suffix = f"-{count+1}"
            code = f"{normalized[:50-len(suffix)]}{suffix}"
        self.encounter_code_tracker[normalized] = count + 1
        return code
    
    def preload_master_data(self, df):
        """Pre-load all master data before processing rows"""
        logger.info("[STEP] Pre-loading master data...")
        
        try:
            institutions = df['InstitutionName'].dropna().unique()
            for inst in institutions:
                location = df[df['InstitutionName'] == inst]['Location'].iloc[0]
                self.master.get_facility(str(inst).strip(), str(location).strip())
            logger.info(f"  ✓ {len(self.master.facility_cache)} facilities")
            
            modalities = df['Modality'].dropna().unique()
            for mod in modalities:
                self.master.get_modality(str(mod).strip())
            logger.info(f"  ✓ {len(self.master.modality_cache)} modalities")

            projections = df['Projection'].dropna().unique()
            for proj in projections:
                self.master.get_projection(str(proj).strip())
            logger.info(f"  ✓ {len(self.master.projection_cache)} projections")
            
            all_diags = set()
            for labels_str in df['Labels'].dropna().unique():
                for diag in str(labels_str).split('|'):
                    all_diags.add(diag.strip())
            
            for diag in all_diags:
                if diag:
                    self.master.get_diagnosis(diag)
            logger.info(f"  ✓ {len(self.master.diagnosis_cache)} diagnoses")

            unique_regions = {}
            for labels_str in df['Labels'].dropna().unique():
                region = infer_region(str(labels_str))
                unique_regions[region['code']] = region
            if not unique_regions:
                unique_regions[DEFAULT_REGION['code']] = DEFAULT_REGION
            for region in unique_regions.values():
                self.master.get_region(region['code'], region['name'], region['body_system'])
            logger.info(f"  ✓ {len(self.master.region_cache)} anatomical regions\n")
        except Exception as e:
            logger.error(f"[FAIL] Pre-load failed: {e}")
            raise
    
    def process_row(self, row, idx: int) -> bool:
        try:
            pid = str(row.get('PatientID', f'PAT{idx:06d}')).strip()[:50]
            dob = parse_date(row.get('DateOfBirth')) or date(1970, 1, 1)
            study_dt = parse_datetime(row.get('StudyDate')) or datetime.now()
            study_identifier = normalize_text(row.get('StudyID', f"{pid}-{idx}"), f"{pid}-{idx}")
            sex = normalize_text(row.get('PatientSex', 'M'), 'M')[:10]
            height = safe_int_value(row.get('PatientHeight'))
            weight = safe_int_value(row.get('PatientWeight'))
            location = normalize_text(row.get('Location', 'Unknown'), 'Unknown')[:255]
            inst = normalize_text(row.get('InstitutionName', 'Hospital'), 'Hospital')[:255]
            mod = normalize_text(row.get('Modality', 'DX'), 'DX')[:20]
            projection_code = normalize_text(row.get('Projection', 'PA'), 'PA').upper()
            imgid = normalize_text(row.get('ImageID', f'IMG{idx:06d}'), f'IMG{idx:06d}')[:100]
            report = normalize_text(row.get('Report', ''), '')
            findings = normalize_text(row.get('Findings', ''), '')
            impression = normalize_text(row.get('Impression', ''), '')
            labels = normalize_text(row.get('Labels', ''), '')
            
            if not impression and findings:
                impression = f"Impression autogenerated from findings: {findings[:200]}"

            fid = self.master.get_facility(inst, location)
            mid = self.master.get_modality(mod)
            projection_id = self.master.get_projection(projection_code)
            region_source = labels or findings or impression
            region_meta = infer_region(region_source)
            region_id = self.master.get_region(region_meta['code'], region_meta['name'], region_meta['body_system'])

            result = self.db.fetch_one(
                "SELECT patient_id FROM operational.patients WHERE patient_code = %s",
                (pid,)
            )

            if result:
                patid = result[0]
            else:
                patid = str(uuid.uuid4())
                self.db.execute(
                    """
                    INSERT INTO operational.patients (
                        patient_id, patient_code, date_of_birth, age, sex, height, weight,
                        geographic_location, first_encounter_date, last_encounter_date, total_encounters
                    ) VALUES (
                        %s, %s, %s,
                        CASE WHEN %s IS NULL THEN NULL
                             ELSE EXTRACT(YEAR FROM AGE(CURRENT_DATE, %s::date))::INT
                        END,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (patid, pid, dob, dob, dob, sex, height, weight, location, study_dt, study_dt, 1)
                )

            base_encounter_code = study_identifier[:50] if study_identifier else f"{pid}-{idx}"
            encounter_code = self._generate_encounter_code(base_encounter_code)
            encid = str(uuid.uuid4())
            
            self.db.execute(
                """
                INSERT INTO operational.encounters (
                    encounter_id, patient_id, facility_id, encounter_code, encounter_date, referring_physician, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (encid, patid, fid, encounter_code, study_dt, get_random_name(PHYSICIAN_NAMES, prefix='Dr. '), summarize_text(impression or findings))
            )

            procid = str(uuid.uuid4())
            self.db.execute(
                """
                INSERT INTO operational.procedures (
                    procedure_id, encounter_id, modality_id, projection_id, region_id, procedure_date, procedure_notes, technician_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (procid, encid, mid, projection_id, region_id, study_dt, summarize_text(findings), get_random_name(TECHNICIAN_NAMES))
            )

            imgfn = imgid.split('/')[-1] if '/' in imgid else imgid
            self.db.execute(
                """
                INSERT INTO operational.radiological_images (image_id, procedure_id, image_code, image_filename, image_path, studyid)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (str(uuid.uuid4()), procid, imgid, imgfn, imgid, study_identifier or None)
            )

            if report:
                result = self.db.fetch_one(
                    "SELECT language_id FROM master.language_registry WHERE language_code = 'en'"
                )
                lang_id = result[0] if result else None
                if lang_id:
                    word_count = len(report.split())
                    review_date = study_dt + timedelta(hours=random.randint(1, 48))
                    self.db.execute(
                        """
                        INSERT INTO operational.clinical_reports (
                            report_id, procedure_id, language_id, report_text, report_type,
                            word_count, reviewed_by, review_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(uuid.uuid4()),
                            procid,
                            lang_id,
                            report,
                            'Radiology Report',
                            word_count,
                            get_random_name(REVIEWER_NAMES, prefix='Dr. '),
                            review_date
                        )
                    )

            findings_text = findings or "No abnormal findings reported."
            severity = derive_finding_severity(labels)
            abnormality = severity != 'Normal'
            confidence = round(random.uniform(0.7, 0.95), 2) if abnormality else round(random.uniform(0.9, 0.99), 2)

            self.db.execute(
                """
                INSERT INTO operational.findings (
                    finding_id, procedure_id, findings_text, impression_text, finding_severity,
                    abnormality_detected, confidence_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    procid,
                    findings_text,
                    impression if impression else None,
                    severity,
                    abnormality,
                    confidence
                )
            )

            diagnoses = [diag.strip() for diag in labels.split('|') if diag.strip()]
            for seq, diag in enumerate(diagnoses, 1):
                did = self.master.get_diagnosis(diag)
                diag_conf = round(random.uniform(0.65, 0.98), 2)
                self.db.execute(
                    """
                    INSERT INTO operational.procedure_diagnosis (
                        pd_id, procedure_id, diagnosis_id, diagnosis_sequence, is_primary, confidence_score
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (str(uuid.uuid4()), procid, did, seq, seq == 1, diag_conf)
                )

            self.stats['success'] += 1
            return True

        except Exception as e:
            logger.error(f"Row {idx} failed: {str(e)[:150]}")
            self.db.rollback()
            self.stats['failed'] += 1
            self.stats['errors'].append({'row': idx, 'error': str(e)[:200]})
            return False
    
    def run(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the ETL pipeline and return statistics"""
        try:
            logger.info("="*80)
            logger.info("eFICHE ETL PIPELINE - AIRFLOW INTEGRATION")
            logger.info("="*80 + "\n")
            
            if not self.db.connect():
                raise Exception("Database connection failed")
            
            df = self.load_csv()
            self.stats['total'] = len(df)
            
            if limit:
                df = df.head(limit)
                logger.info(f"Processing first {limit} rows\n")
            
            self.preload_master_data(df)
            
            logger.info(f"Processing {len(df):,} rows...\n")
            for batch_start in range(0, len(df), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                batch_success = 0
                for idx_in_batch, (idx, row) in enumerate(batch_df.iterrows(), 1):
                    row_num = batch_start + idx_in_batch
                    if self.process_row(row, row_num):
                        batch_success += 1
                
                if self.db.commit():
                    logger.info(f"[OK] Batch {batch_start//BATCH_SIZE + 1}: {batch_success}/{len(batch_df)} success | Total: {self.stats['success']}/{batch_end}")
                else:
                    logger.error(f"Batch commit failed")
                    break
            
            logger.info("\n" + "="*80)
            logger.info(f"RESULT: {self.stats['success']}/{self.stats['total']} rows loaded successfully")
            logger.info(f"Success Rate: {100*self.stats['success']/max(1,self.stats['total']):.1f}%")
            logger.info("="*80)
            
            return self.stats
        
        except Exception as e:
            logger.error(f"ETL failed: {e}")
            self.stats['error'] = str(e)
            return self.stats
        finally:
            self.db.close()

# =============================================================================
# AIRFLOW INTEGRATION FUNCTION
# =============================================================================

def load_data_to_database(
    csv_file: str = None,
    batch_size: int = None,
    db_host: str = None,
    db_port: int = None,
    db_user: str = None,
    db_password: str = None,
    db_name: str = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Airflow-compatible function to load data
    Can be called directly from DAG
    
    Args:
        csv_file: Path to CSV file
        batch_size: Rows per batch
        db_*: Database credentials (uses env vars if not provided)
        **kwargs: Additional Airflow context
    
    Returns:
        Dictionary with load statistics
    """
    
    _csv_file = csv_file or ETL_CSV_FILE
    _batch_size = batch_size or BATCH_SIZE
    _db_host = db_host or DB_HOST
    _db_port = db_port or DB_PORT
    _db_user = db_user or DB_USER
    _db_password = db_password or DB_PASSWORD
    _db_name = db_name or DB_NAME
    
    db = DatabaseManager(_db_host, _db_port, _db_user, _db_password, _db_name)
    pipeline = ETLPipeline(_csv_file, db)
    
    return pipeline.run()

# =============================================================================
# MAIN (for standalone testing)
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='eFICHE ETL Pipeline')
    parser.add_argument('--csv', default=ETL_CSV_FILE, help='CSV file path')
    parser.add_argument('--limit', type=int, default=None, help='Limit rows to process')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Batch size')
    args = parser.parse_args()
    
    db = DatabaseManager(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
    pipeline = ETLPipeline(args.csv, db)
    result = pipeline.run(limit=args.limit)
    
    sys.exit(0 if result.get('success', 0) > 0 else 1)