# Treasure Data POC Workflows

A comprehensive collection of Proof of Concept (POC) workflows for Treasure Data's Customer Data Platform, including data unification, analytics, and AI-powered agents.

## 🚀 Recent Improvements

This repository has been enhanced with several professional-grade improvements to increase code quality, maintainability, and reliability:

### 🔧 Code Quality Enhancements

- **Fixed Critical Python Syntax Error**: Corrected `else if` → `elif` syntax error in `general_api_export/api_export.py`
- **Eliminated Code Duplication**: Created a unified API client in `common/td_api_client.py` to replace duplicated code across modules
- **Enhanced Error Handling**: Implemented robust error handling with custom exceptions and detailed error messages
- **Professional Logging**: Added structured logging throughout the codebase for better debugging and monitoring

### 🏗️ Architecture Improvements  

- **Common Module**: Created `common/` directory with shared utilities and API clients
- **Retry Logic**: Implemented automatic retry mechanisms for API calls with exponential backoff
- **Input Validation**: Added comprehensive validation for environment variables and function parameters
- **Regional Support**: Enhanced support for both US and EU Treasure Data regions

### 📈 Performance & Reliability

- **Request Timeouts**: Added configurable timeouts to prevent hanging requests
- **Session Management**: Implemented connection pooling and session reuse for better performance
- **Chunk Processing**: Improved data processing with progress tracking and error recovery
- **Resource Management**: Better handling of API rate limits and resource cleanup

## 📁 Project Structure

```
poc_workflows/
├── common/                          # Shared utilities and API clients
│   ├── td_api_client.py            # Unified TD API client with error handling
│   ├── requirements.txt            # Common dependencies
│   └── __init__.py                 # Module initialization
├── staging_agent/                  # Data cleaning and standardization agent
├── knowledge_base_db/              # Knowledge base creation tools  
├── general_api_export/             # Generic API export utilities
├── advanced_probabalistic/         # Advanced probabilistic unification
├── incremental/                    # Incremental data processing workflows
└── standard_poc_workflow/          # Standard POC workflow templates
```

## 🛠️ Key Workflows

### 1. Staging Agent (`staging_agent/`)
Creates AI-powered agents for data cleaning and standardization with automatic schema detection and transformation suggestions.

**Features:**
- Automatic table discovery
- AI-generated staging queries
- Best practice transformations
- Support for US/EU regions

### 2. Knowledge Base DB (`knowledge_base_db/`)  
Builds knowledge bases from existing Treasure Data databases for AI agent integration.

**Features:**
- Bulk table import
- Automated knowledge base creation
- Project management integration

### 3. General API Export (`general_api_export/`)
Generic utility for exporting data from Treasure Data to external APIs with robust error handling.

**Features:**
- Configurable batch processing
- Comprehensive error handling
- Progress tracking
- Automatic retry logic

## 🚦 Getting Started

### Prerequisites

- Python 3.8+
- Treasure Data API key
- Required dependencies (see individual `requirements.txt` files)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd poc_workflows
```

2. Install common dependencies:
```bash
pip install -r common/requirements.txt
```

### Usage Examples

#### Creating a Staging Agent
```bash
cd staging_agent/src
python main.py <td_api_key> <database_name> [region]
```

#### Creating a Knowledge Base
```bash
cd knowledge_base_db/src  
python main.py <project_id> <td_api_key> <database_name> <kb_name> [region]
```

#### Exporting Data via API
```bash
cd general_api_export
# Set environment variables
export TD_API_KEY="your_api_key"
export TD_DATABASE="your_database"
export TD_TABLE="your_table"
export TD_COLUMNS="col1,col2,col3"
# ... other required env vars

python api_export.py
```

## 🔧 Configuration

### Environment Variables

Most workflows use environment variables for configuration:

- `TD_API_KEY`: Your Treasure Data API key
- `TD_REGION`: Region ("us" or "eu") 
- `TD_DATABASE`: Source database name
- `TD_TABLE`: Source table name
- Additional workflow-specific variables

### Error Handling

All modules now include comprehensive error handling:

- **TreasureDataAPIError**: Custom exception for API-related errors
- **Automatic Retry**: Built-in retry logic for transient failures
- **Detailed Logging**: Structured logs for debugging and monitoring
- **Graceful Degradation**: Proper cleanup on failures

## 📊 Monitoring & Observability

The enhanced workflows provide detailed logging and monitoring capabilities:

- **Structured Logging**: Consistent log format across all modules
- **Progress Tracking**: Real-time progress updates for long-running operations
- **Error Classification**: Categorized error types for easier troubleshooting
- **Performance Metrics**: Processing speed and success rate tracking

## 🔄 Workflow Types

### Standard POC Workflow
Basic customer data platform setup with unification and analytics.

### Incremental Workflow  
Optimized for incremental data processing and updates.

### Advanced Probabilistic
Advanced ML-based unification with probabilistic matching.

## 🤝 Contributing

When contributing to this repository:

1. Follow the established code structure in `common/`
2. Use the shared API client for Treasure Data interactions
3. Include comprehensive error handling and logging
4. Add tests for new functionality
5. Update documentation for any API changes

## 📝 License

Copyright © 2024 Treasure Data, Inc. All rights reserved.

## 🆘 Support

For technical support:
- Review workflow logs for detailed error information
- Check the common API client documentation
- Contact your Treasure Data representative for platform-specific issues

---

*This repository showcases professional software engineering practices including code reusability, error handling, logging, and documentation.*
