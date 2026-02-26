# Blue Thumb Water Quality Dashboard

## Overview

This project transforms complex water quality datasets from Oklahoma's Blue Thumb volunteer monitoring program into accessible, interactive visualizations that help communicate stream health across Oklahoma's watersheds. With data from **370+ monitoring sites**, the dashboard provides comprehensive statewide coverage enhanced by automated cloud processing and AI-powered assistance.

## Technology Stack

### Core Platform
- **Python 3.12+** - Data processing and analysis
- **Dash & Plotly** - Interactive web dashboard and visualizations  
- **SQLite** - Normalized database schema for comprehensive monitoring data
- **Pandas** - Data manipulation and analysis
- **Bootstrap** - Responsive UI components

### Google Cloud Integration
- **Cloud Functions** - Serverless data processing and synchronization
- **Cloud Storage** - Database hosting with automated backups
- **Cloud Scheduler** - Automated daily data updates
- **ArcGIS API** - Survey123 integration for real-time data collection
- **Vertex AI** - AI-powered stream health chatbot with document grounding

## Key Features

### Interactive Statewide Site Map
- Real-time visualization of all 370+ monitoring sites across Oklahoma
- Parameter-based color coding for immediate status assessment  
- Active site filtering to focus on currently monitored locations
- Click-to-navigate functionality for detailed site analysis

### Cloud-Powered Data Pipeline
- **Dual Sync Modes**: Survey123 authenticated API and public ArcGIS FeatureServer
- **Real-Time FeatureServer Sync**: Incremental sync via EditDate watermarks with idempotent insertion
- **Live Database Refresh**: Cloud Run automatically detects and downloads updated databases from GCS
- **Smart Data Processing**: Handles range-based measurements and validation
- **Backup Management**: Automatic database backups before each update
- **Cost-Efficient**: <$10/month operational costs

### Comprehensive Chemical Analysis
- Time series visualization of key water quality parameters
- Reference threshold highlighting (normal, caution, poor conditions)
- Multi-site comparison capabilities
- Seasonal filtering and trend analysis
- Parameter-specific educational explanations

### Biological Community Assessment
- Fish community integrity scoring over time
- Macroinvertebrate bioassessment results statewide
- Species diversity metrics and trends
- Detailed biological metrics for scientific review
- Interactive species galleries with identification guides

### Habitat Assessment
- Physical stream condition scoring across Oklahoma watersheds
- Habitat quality trends over monitoring periods
- Component-level habitat metrics breakdown
- Watershed-scale habitat comparisons

### AI Stream Health Assistant
- **Expert Knowledge**: Trained on Blue Thumb documentation and stream health science
- **Context-Aware**: Provides tab-specific guidance and answers
- **Multi-Source**: Combines grounded knowledge with real-time search capabilities
- **Interactive Chat**: Available on every tab with persistent conversation history

## Project Structure

```
├── app.py                
├── requirements.txt       
├── cloud_functions/
│   └── survey123_sync/    # Automated data synchronization (Survey123 + FeatureServer modes)
│       ├── main.py        # Dual-mode entry point
│       ├── chemical_processor.py
│       ├── deploy.sh      # Stages shared modules for deployment
│       └── requirements.txt
├── database/             # Database schema, connections, GCS-backed refresh
│   ├── db_schema.py
│   ├── database.py       # Connection management with Cloud Run GCS refresh
│   └── reset_database.py
├── data_processing/      # Comprehensive data cleaning and processing pipeline
│   ├── data_loader.py
│   ├── site_processing.py
│   ├── chemical_processing.py
│   ├── arcgis_sync.py    # Real-time ArcGIS FeatureServer sync
│   ├── fish_processing.py
│   ├── macro_processing.py
│   └── habitat_processing.py 
├── callbacks/             # Interactive dashboard logic
│   ├── chatbot_callbacks.py 
│   ├── chemical_callbacks.py
│   ├── biological_callbacks.py
│   └── habitat_callbacks.py
├── layouts/           
│   ├── components/     
│   │   └── chatbot.py   
│   └── tabs/          
├── visualizations/      
├── data/
│   ├── raw/            # Source CSV files (not included — see Data Access)
│   ├── interim/        # Cleaned and validated data
│   └── processed/      # Database-ready outputs
│       └── chatbot_data/ # AI knowledge base content
├── text/              
└── assets/           
```

## Quick Start

### Prerequisites
- Python 3.12+
- Git
- Google Cloud SDK (for cloud features)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Blue-Thumb-Dashboard.git
   cd Blue-Thumb-Dashboard
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Obtain the source data**

   Raw CSV files are not included in the repository. See the [Data Access](#data-access) section below for download links and API details.

   Place downloaded CSV files in `data/raw/` to match the expected directory structure.

5. **Load the monitoring data**
   ```bash
   python -m database.reset_database
   ```

6. **Start the dashboard**
   ```bash
   python app.py
   ```

7. **Open your browser**
   Navigate to http://127.0.0.1:8050

## Technical Highlights

### Data Processing Pipeline
- **ETL Architecture**: Comprehensive processes for multiple data types across 370+ sites
- **Dual Real-time Integration**: Survey123 API and public ArcGIS FeatureServer sync with idempotent insertion
- **Data Validation**: Duplicate detection, QAQC gating, and quality assurance
- **Scalable Design**: Cloud-native architecture with live database refresh on Cloud Run

### Testing & Quality Assurance
- **Comprehensive Test Suite**: 700+ tests ensuring reliability across all components
- **Automated CI/CD**: Continuous integration with quality checks
- **Data Validation**: Multi-layer validation ensuring data integrity
- **Performance Monitoring**: Real-time tracking of system performance

### AI-Powered User Experience  
- **Contextual Assistance**: Context-aware chatbot providing relevant stream health guidance
- **Knowledge Grounding**: Responses based on authoritative Blue Thumb documentation
- **Intelligent Fallback**: Google Search integration for comprehensive coverage
- **Natural Interaction**: Conversational interface with typing indicators and message history

## Impact & Results

- **370+ Monitoring Sites**: Comprehensive statewide water quality coverage
- **Multi-Parameter Analysis**: Chemical, biological, and habitat assessment integration
- **Educational Outreach**: Public-facing dashboard promoting stream health awareness
- **Automated Processing**: Daily data updates preventing need for manual intervention 
- **AI Enhancement**: Intelligent assistance improving user engagement and understanding

## Future Enhancements

- [ ] **Advanced Analytics**: Machine learning for trend prediction and anomaly detection
- [ ] **Mobile Optimization**: Progressive web app capabilities
- [ ] **Multi-State Expansion**: Framework for other volunteer monitoring programs
- [ ] **API Development**: Public API for researchers and third-party applications

## Data Access

This dashboard uses data from the [Blue Thumb Volunteer Stream Monitoring Program](https://www.bluethumbok.com/), administered by the Oklahoma Conservation Commission. Raw data files are not included in this repository and must be obtained separately.

### Legacy Data (Biological, Habitat, and Historical Chemical)

All biological, habitat, and legacy chemical monitoring data can be downloaded as CSV files from the OCC Water Quality portal:

https://occwaterquality.shinyapps.io/OCC-app23b/

Place downloaded files in `data/raw/` and run `python -m database.reset_database` to build the local database.

### Updated Chemical Data (ArcGIS FeatureServer)

Current chemical monitoring data is collected via ArcGIS Survey123 and served through a public FeatureServer REST API. No authentication is required.

**REST API endpoint:**
```
https://services5.arcgis.com/L6JGkSUcgPo1zSDi/arcgis/rest/services/bluethumb_oct2020_view/FeatureServer/0/query
```

Example query to fetch recent records:
```
?where=1%3D1&outFields=*&resultRecordCount=10&orderByFields=EditDate+DESC&f=json
```

The dashboard's Cloud Function uses this API for automated daily syncs (see `data_processing/arcgis_sync.py`). You can also browse the data interactively via the [Blue Thumb Web Map](https://okconservation.maps.arcgis.com/apps/webappviewer/index.html?id=1654493dccdd42c29d170785c6b242bf).

## Acknowledgments

- **Blue Thumb Program** - Oklahoma Conservation Commission
- **Volunteer Monitors** - Citizens collecting water quality data across 370+ sites statewide

---

*Built with ❤️ for Oklahoma's streams and the volunteers who protect them*


