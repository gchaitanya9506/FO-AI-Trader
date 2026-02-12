# F&O AI Trader 🚀

**Advanced Automated Options Trading System with Multi-Indicator Signal Generation**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](./tests/)

F&O AI Trader is a sophisticated automated trading system that combines real-time NSE option chain data with advanced technical indicators to generate intelligent trading signals. The system features automated data pipelines, machine learning predictions, and real-time Telegram notifications.

## 🎯 Key Features

- **🔄 Automated Data Pipeline**: Real-time NSE option chain data fetching and processing
- **📊 Advanced Signal Generation**: Multi-indicator system using PCR + RSI + Open Interest analysis
- **⏰ Signal Expiry Tracking**: 15-minute signal validity with automatic renewal system
- **📱 Smart Alerting**: Telegram notifications with detailed signal information
- **🤖 Machine Learning**: XGBoost-based prediction models for enhanced accuracy
- **🌐 Web Dashboard**: Flask-based monitoring interface with live suggestions
- **🏥 Health Monitoring**: Comprehensive system health checks and data quality validation
- **⚡ Zero Manual Intervention**: Fully automated end-to-end pipeline

## 🏗️ System Architecture

```
NSE API → Data Processor → Database → Signal Engine → Telegram Alerts
    ↓           ↓            ↓           ↓             ↓
Raw Data → Cleaned Data → Storage → ML Signals → User Notifications
    ↓           ↓            ↓           ↓             ↓
YFinance → Validation → Features → Monitoring → Web Dashboard
```

## 📋 Prerequisites

- **Python 3.11+** with pip package manager
- **Git** for repository cloning
- **Telegram account** for receiving alerts
- **Internet connection** for real-time data fetching
- **4GB+ RAM** recommended for optimal performance

## ⚡ Quick Start

Get the F&O AI Trader up and running in under 5 minutes:

```bash
# 1. Clone the repository
git clone <repository-url>
cd fo-ai-trader

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env file with your Telegram credentials

# 5. Deploy with one command
python deploy_automation.py
```

That's it! The system will automatically:
- ✅ Check all dependencies
- ✅ Set up directories and database
- ✅ Run validation tests
- ✅ Start the automated pipeline

## 🔧 Detailed Installation

### Step 1: System Requirements

Ensure you have Python 3.11 or higher installed:

```bash
python3 --version  # Should show 3.11.0 or higher
```

### Step 2: Repository Setup

```bash
git clone <repository-url>
cd fo-ai-trader

# Create virtual environment (highly recommended)
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

# Upgrade pip to latest version
pip install --upgrade pip
```

### Step 3: Install Dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# Optional: Install NSE data source (if available)
pip install nsepython  # For enhanced NSE data access
```

### Step 4: Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit the .env file with your configuration
nano .env  # or use your preferred editor
```

Required environment variables:
```bash
# Telegram Bot Configuration
TELEGRAM_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### Step 5: Database Initialization

```bash
# Initialize the database schema
python -c "from database.db_manager import init_option_chain_table; init_option_chain_table()"
```

## 🤖 Telegram Integration Setup

### Creating a Telegram Bot

1. **Open Telegram** and search for `@BotFather`
2. **Start a chat** and send `/newbot`
3. **Choose a name** for your bot (e.g., "My F&O Trader")
4. **Choose a username** (must end with 'bot', e.g., "my_fo_trader_bot")
5. **Save the token** provided by BotFather

### Getting Your Chat ID

1. **Send a message** to your newly created bot
2. **Visit this URL** (replace YOUR_BOT_TOKEN):
   ```
   https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates
   ```
3. **Find your chat ID** in the response JSON under `message.chat.id`
4. **Add both values** to your `.env` file

### Test Telegram Setup

```bash
# Test your Telegram configuration
python -c "
from execution.alert_system import AlertSystem
alert = AlertSystem()
alert.send_alert('🧪 Test Alert', 'F&O AI Trader setup complete!')
"
```

## 🚀 Running the System

### Option 1: Automated Deployment (Recommended)

```bash
python deploy_automation.py
```

This comprehensive script will:
- ✅ Check all dependencies
- ✅ Set up required directories
- ✅ Initialize the database
- ✅ Run validation tests
- ✅ Start the automation system

### Option 2: Individual Components

#### Web Dashboard
```bash
python app.py
# Access at http://localhost:8080/suggestions
```

#### Signal Monitoring System
```bash
python strategy/signal_monitor.py
# Runs continuously, monitoring for trading signals
```

#### Data Pipeline
```bash
python data_ingestion/automated_scheduler.py
# Automated data fetching during market hours
```

#### System Health Monitoring
```bash
python monitoring/pipeline_monitor.py
# Continuous health checks and alerts
```

## ⚙️ Configuration

### Main Configuration File

Edit `config/settings.yaml` to customize system behavior:

```yaml
# API Configuration
api:
  nse_rate_limit: 30        # calls per minute
  telegram_rate_limit: 20   # calls per minute
  request_timeout: 10       # seconds

# Trading Strategy
trading:
  default_symbol: NIFTY
  signal_confidence_threshold: 0.6
  features:
    - ema9
    - ema21
    - rsi
    - atr
    - vwap

# Data Pipeline
data_pipeline:
  fetch_interval_seconds: 300    # 5 minutes
  market_hours:
    start_time: "09:15"          # NSE market hours
    end_time: "15:30"
    timezone: "Asia/Kolkata"
```

### Signal Configuration

The system uses sophisticated multi-indicator analysis:

#### PCR (Put-Call Ratio) Signals
```yaml
pcr_thresholds:
  buy_ce_max: 0.7     # PCR below 0.7 → Consider BUY CE
  buy_pe_min: 1.3     # PCR above 1.3 → Consider BUY PE
  neutral_range: [0.8, 1.2]  # No signals in this range
```

#### RSI (Relative Strength Index) Signals
```yaml
rsi_levels:
  oversold_max: 30    # RSI < 30 = oversold condition
  overbought_min: 70  # RSI > 70 = overbought condition
```

#### Open Interest Analysis
```yaml
oi_analysis:
  significant_change_pct: 15     # 15% OI change threshold
  volume_spike_multiplier: 2     # 2x average volume = spike
  min_oi_level: 10000           # Minimum OI to consider
```

#### Signal Management
```yaml
signal_cooldown_minutes: 15      # Signal validity period
confidence_threshold: 0.7        # Minimum confidence to send
max_signals_per_hour: 6         # Rate limiting
```

## 📊 System Components

### Database Schema

The system uses SQLite with optimized schemas:

```sql
-- Option Chain Data Table
CREATE TABLE option_chain_data (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    strike_price REAL,
    option_type TEXT,  -- CE/PE
    last_price REAL,
    open_interest INTEGER,
    volume INTEGER,
    implied_volatility REAL,
    delta REAL,
    gamma REAL,
    theta REAL,
    vega REAL,
    timestamp DATETIME,
    date TEXT
);

-- Signal Tracking Table
CREATE TABLE signal_tracking (
    id INTEGER PRIMARY KEY,
    signal_type TEXT,
    symbol TEXT,
    strike_price REAL,
    confidence REAL,
    created_at DATETIME,
    expires_at DATETIME,
    is_active BOOLEAN,
    renewed_count INTEGER
);
```

### Data Flow Pipeline

1. **Data Ingestion** (`data_ingestion/`)
   - `automated_scheduler.py`: Market-aware scheduling
   - `data_processor.py`: Unified data cleaning and validation
   - `pipeline_manager.py`: End-to-end orchestration

2. **Signal Generation** (`strategy/`)
   - `advanced_signal_engine.py`: Multi-indicator analysis
   - `signal_monitor.py`: Real-time monitoring daemon
   - `signal_formatter.py`: Telegram message formatting

3. **Execution** (`execution/`)
   - `alert_system.py`: Telegram notification delivery

4. **Monitoring** (`monitoring/`)
   - `pipeline_monitor.py`: Health checks and data quality

## 🧪 Testing and Validation

### Running the Test Suite

```bash
# Run all tests with coverage report
./run_tests.sh

# Run specific test module
./run_tests.sh --module test_database

# Run without coverage analysis
./run_tests.sh --no-coverage

# Run with custom verbosity
./run_tests.sh --verbosity 2
```

### Test Categories

- **🗄️ Database Tests** (`tests/test_database.py`)
  - Connection handling
  - Schema validation
  - Data integrity

- **⚙️ Configuration Tests** (`tests/test_configuration.py`)
  - Settings validation
  - Environment variables
  - YAML parsing

- **📊 Data Validation** (`tests/test_data_validation.py`)
  - Data quality checks
  - Schema compliance
  - Outlier detection

- **🔔 Alert System Tests** (`tests/test_alerts.py`)
  - Telegram integration
  - Message formatting
  - Error handling

- **🔗 Integration Tests** (`tests/test_integration.py`)
  - End-to-end workflows
  - Component interaction
  - Error scenarios

### Validation Scripts

```bash
# Configuration validation
python -c "from config.app_config import get_config; print('✅ Config valid')"

# Database connectivity
python -c "from database.db_manager import get_conn; print('✅ DB connected' if get_conn() else '❌ DB failed')"

# Pipeline validation (comprehensive test)
python test_automated_pipeline.py
```

## 🔍 Monitoring and Maintenance

### Health Monitoring

The system includes comprehensive health checks:

```bash
# Start health monitoring daemon
python monitoring/pipeline_monitor.py
```

**Monitored Components:**
- ✅ Database connectivity and schema
- ✅ API endpoints availability and rate limits
- ✅ Data freshness and quality metrics
- ✅ Signal generation performance
- ✅ Telegram bot connectivity
- ✅ Disk space and system resources

### Log Management

**Log Locations:**
```
logs/
├── app.log              # Web application logs
├── data_pipeline.log    # Data ingestion logs
├── signal_engine.log    # Signal generation logs
├── alerts.log           # Telegram notification logs
└── monitoring.log       # Health check logs
```

**Log Configuration:**
```yaml
logging:
  level: INFO
  log_to_file: true
  log_to_console: true
  max_file_size_mb: 10
  backup_count: 5
```

### Data Maintenance

```bash
# Check data quality
python -c "
from database.db_manager import check_data_quality
result = check_data_quality()
print(f'Data quality: {result}')
"

# Clean old data (configurable retention)
python -c "
from database.db_manager import cleanup_old_data
cleaned = cleanup_old_data()
print(f'Cleaned {cleaned} old records')
"
```

## 🛠️ Development and Contributing

### Development Setup

```bash
# Install development dependencies
pip install pytest pytest-cov black flake8

# Set up pre-commit hooks (optional)
pip install pre-commit
pre-commit install
```

### Code Quality Standards

```bash
# Format code with Black
black .

# Lint with flake8
flake8 .

# Run type checking (if mypy is installed)
mypy .
```

### Project Structure Understanding

```
fo-ai-trader/
├── app.py                    # Flask web dashboard
├── deploy_automation.py      # One-click deployment
├── requirements.txt          # Python dependencies
├── config/                   # Configuration management
│   ├── settings.yaml        # Main configuration
│   ├── app_config.py        # Config loader
│   └── logging_config.py    # Logging setup
├── data_ingestion/          # Automated data pipeline
│   ├── automated_scheduler.py
│   ├── data_processor.py
│   └── pipeline_manager.py
├── database/                # Data management
│   └── db_manager.py
├── strategy/                # Signal generation
│   ├── advanced_signal_engine.py
│   ├── signal_monitor.py
│   └── signal_formatter.py
├── execution/               # Alert system
│   └── alert_system.py
├── monitoring/              # Health monitoring
│   └── pipeline_monitor.py
├── models/                  # Machine learning
│   └── train_model.py
└── tests/                   # Test suite
    ├── test_database.py
    ├── test_configuration.py
    └── test_integration.py
```

### Extension Points

#### Adding New Technical Indicators

1. **Extend the features list** in `config/settings.yaml`:
```yaml
trading:
  features:
    - ema9
    - ema21
    - rsi
    - your_new_indicator  # Add here
```

2. **Implement the indicator** in the data processing pipeline:
```python
# In data_ingestion/data_processor.py
def calculate_your_indicator(df):
    # Your indicator logic here
    return indicator_values
```

#### Adding New Signal Strategies

1. **Create a new signal engine** in `strategy/`:
```python
# strategy/custom_signal_engine.py
from strategy.signal_config import SignalConfig

class CustomSignalEngine:
    def __init__(self):
        self.config = SignalConfig()

    def generate_signals(self, data):
        # Your custom logic
        return signals
```

2. **Register in the signal monitor**:
```python
# In strategy/signal_monitor.py
from strategy.custom_signal_engine import CustomSignalEngine

# Add to monitoring loop
custom_engine = CustomSignalEngine()
```

#### Adding New Data Sources

1. **Implement data fetcher**:
```python
# data_ingestion/custom_data_source.py
class CustomDataSource:
    def fetch_data(self, symbol):
        # Your data fetching logic
        return data
```

2. **Integrate with processor**:
```python
# In data_ingestion/data_processor.py
from data_ingestion.custom_data_source import CustomDataSource

# Add to data sources
self.custom_source = CustomDataSource()
```

## 🚨 Troubleshooting

### Common Issues and Solutions

#### 1. Dependency Installation Issues

**Problem**: Package installation fails
```bash
ERROR: Could not install packages due to an EnvironmentError
```

**Solution**:
```bash
# Upgrade pip and try again
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# For permission issues (Linux/macOS)
pip install --user -r requirements.txt
```

#### 2. Database Connection Errors

**Problem**: SQLite database connection fails
```bash
sqlite3.OperationalError: database is locked
```

**Solutions**:
```bash
# Check for running processes
ps aux | grep python

# Kill hanging processes
killall python3

# Reset database (will lose data)
rm database/market_data.db
python -c "from database.db_manager import init_option_chain_table; init_option_chain_table()"
```

#### 3. Telegram Bot Issues

**Problem**: Telegram alerts not working

**Common Causes & Solutions**:
```bash
# Invalid token format
# Ensure token format: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz

# Wrong chat ID
# Get correct chat ID from: https://api.telegram.org/botTOKEN/getUpdates

# Rate limiting
# Check logs for rate limit errors, reduce alert frequency
```

#### 4. NSE API Rate Limiting

**Problem**: Too many API requests
```bash
requests.exceptions.HTTPError: 429 Too Many Requests
```

**Solution**: Adjust rate limits in `config/settings.yaml`:
```yaml
api:
  nse_rate_limit: 20        # Reduce from 30
  request_timeout: 15       # Increase timeout
  retry_delay: 2.0          # Increase delay
```

#### 5. Market Hours Detection

**Problem**: System not detecting market hours correctly

**Solution**: Verify timezone configuration:
```yaml
data_pipeline:
  market_hours:
    timezone: "Asia/Kolkata"  # Ensure correct timezone
    start_time: "09:15"       # NSE start time
    end_time: "15:30"         # NSE end time
```

#### 6. Signal Not Generating

**Problem**: No signals being generated despite market activity

**Debugging Steps**:
```bash
# Check signal monitoring logs
tail -f logs/signal_engine.log

# Verify data freshness
python -c "
from database.db_manager import get_latest_data_timestamp
print(f'Latest data: {get_latest_data_timestamp()}')
"

# Check signal thresholds
# Review PCR, RSI, and OI thresholds in config/settings.yaml
```

#### 7. Memory Usage Issues

**Problem**: High memory consumption

**Solutions**:
```bash
# Monitor memory usage
python -c "
import psutil
print(f'Memory: {psutil.virtual_memory().percent}%')
"

# Reduce data retention
# Edit config/settings.yaml:
data_pipeline:
  data_retention_days: 7    # Reduce from 30
```

### Performance Optimization

#### Database Optimization
```sql
-- Add indexes for better query performance
CREATE INDEX idx_option_chain_symbol_date ON option_chain_data(symbol, date);
CREATE INDEX idx_option_chain_timestamp ON option_chain_data(timestamp);
CREATE INDEX idx_signal_tracking_active ON signal_tracking(is_active, expires_at);
```

#### Memory Optimization
```python
# In data processing, use chunking for large datasets
chunk_size = 1000
for chunk in pd.read_csv(file, chunksize=chunk_size):
    process_chunk(chunk)
```

#### API Optimization
```yaml
# Optimize API settings in config/settings.yaml
api:
  request_timeout: 10
  max_retries: 3
  retry_delay: 1.0

data_pipeline:
  fetch_interval_seconds: 300  # Don't fetch too frequently
```

## 📚 Frequently Asked Questions

### Q: What data sources does the system support?
**A**: Currently supports:
- **YFinance** (primary, reliable)
- **NSEPython** (optional, for enhanced NSE data)
- **Custom APIs** (extensible architecture)

### Q: Can I run this on a cloud server?
**A**: Yes! The system is designed for cloud deployment:
- Works on AWS EC2, Google Cloud, DigitalOcean
- Use `screen` or `tmux` for persistent sessions
- Consider using Docker for containerization

### Q: How accurate are the trading signals?
**A**: Signal accuracy depends on:
- Market conditions (volatility, trends)
- Configuration thresholds (adjustable)
- Data quality and freshness
- Multi-indicator confluence

The system provides confidence scores - use signals with >70% confidence.

### Q: Can I customize the signal logic?
**A**: Absolutely! The system is highly extensible:
- Modify thresholds in `config/settings.yaml`
- Add custom indicators in data processing
- Create new signal engines in `strategy/`
- Implement custom notification formats

### Q: What happens during market holidays?
**A**: The system automatically:
- Detects market holidays (NSE calendar)
- Suspends data fetching during non-market hours
- Continues monitoring for manual testing
- Resumes automatically when markets reopen

### Q: How do I backup my data?
**A**:
```bash
# Backup database
cp database/market_data.db backup/market_data_$(date +%Y%m%d).db

# Backup configuration
cp -r config/ backup/config_$(date +%Y%m%d)/

# Backup logs (optional)
cp -r logs/ backup/logs_$(date +%Y%m%d)/
```

### Q: Can I use this for other markets (US, Europe)?
**A**: The architecture supports other markets with modifications:
- Update market hours in configuration
- Replace NSE APIs with appropriate data sources
- Adjust timezone settings
- Modify symbol formats

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Commit your changes**: `git commit -m 'Add amazing feature'`
4. **Push to the branch**: `git push origin feature/amazing-feature`
5. **Open a Pull Request**

### Contributing Guidelines

- Follow PEP 8 style guidelines
- Add tests for new features
- Update documentation for any changes
- Ensure all tests pass before submitting PR

## 💬 Support

- **Issues**: Report bugs and request features via GitHub Issues
- **Documentation**: Check this README and inline code documentation
- **Community**: Join discussions in GitHub Discussions

## 🙏 Acknowledgments

- **NSE** for providing market data APIs
- **YFinance** for reliable financial data access
- **Telegram** for the notification platform
- **XGBoost** for machine learning capabilities
- **Flask** for the web framework

---

**⚠️ Disclaimer**: This software is for educational and research purposes only. It is not financial advice. Trading in options involves substantial risk and may not be suitable for all investors. Always consult with a qualified financial advisor before making investment decisions.

**📈 Happy Trading!** 🚀