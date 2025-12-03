import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5434')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin')
DB_NAME = os.getenv('DB_ANALYTICS_NAME', 'efiche_clinical_db_analytics')
OUTPUT_DIR = 'visualizations'

def get_db_connection():
    """Create SQLAlchemy engine"""
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

def ensure_output_dir():
    """Create output directory if it doesn't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def plot_procedures_by_modality(engine):
    """Generate bar chart for procedures by modality"""
    query = """
    SELECT m.modality_name, COUNT(*) as procedure_count
    FROM analytics.fact_procedure fp
    JOIN analytics.dim_modality m ON fp.modality_sk = m.modality_sk
    GROUP BY m.modality_name
    ORDER BY procedure_count DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='modality_name', y='procedure_count', palette='viridis')
    plt.title('Procedures by Modality')
    plt.xlabel('Modality')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/procedures_by_modality.png')
    print(f"Generated {OUTPUT_DIR}/procedures_by_modality.png")

def plot_patient_demographics(engine):
    """Generate pie chart for patient sex distribution"""
    query = """
    SELECT sex, COUNT(*) as patient_count
    FROM analytics.dim_patient
    GROUP BY sex
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    plt.figure(figsize=(8, 8))
    plt.pie(df['patient_count'], labels=df['sex'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
    plt.title('Patient Distribution by Sex')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/patient_sex_distribution.png')
    print(f"Generated {OUTPUT_DIR}/patient_sex_distribution.png")

def plot_procedures_by_facility(engine):
    """Generate horizontal bar chart for procedures by facility"""
    query = """
    SELECT f.facility_name, COUNT(*) as procedure_count
    FROM analytics.fact_procedure fp
    JOIN analytics.dim_facility f ON fp.facility_sk = f.facility_sk
    GROUP BY f.facility_name
    ORDER BY procedure_count DESC
    LIMIT 10
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    plt.figure(figsize=(12, 8))
    sns.barplot(data=df, y='facility_name', x='procedure_count', palette='magma')
    plt.title('Top 10 Facilities by Procedure Volume')
    plt.xlabel('Procedure Count')
    plt.ylabel('Facility')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/procedures_by_facility.png')
    print(f"Generated {OUTPUT_DIR}/procedures_by_facility.png")

def main():
    print("Starting visualization generation...")
    ensure_output_dir()
    
    try:
        engine = get_db_connection()
        
        # Set style
        sns.set_theme(style="whitegrid")
        
        plot_procedures_by_modality(engine)
        plot_patient_demographics(engine)
        plot_procedures_by_facility(engine)
        
        print("\nVisualization complete! Check the 'visualizations' directory.")
        
    except Exception as e:
        print(f"Error generating visualizations: {e}")

if __name__ == "__main__":
    main()
