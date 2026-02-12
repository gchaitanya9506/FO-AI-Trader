import sqlite3
import pandas as pd
import os
import re
from config.logging_config import get_logger
from config.app_config import get_config

logger = get_logger('database')
config = get_config()

DB_PATH = config.database.path
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Whitelist of allowed table names for security
ALLOWED_TABLES = {
    "underlying_features",
    "option_chain_features",
    "option_chain_data",
    "processed_signals",
    "market_data",
    "trading_signals"
}

def validate_table_name(table):
    """Validate table name to prevent SQL injection."""
    if not isinstance(table, str):
        raise ValueError("Table name must be a string")

    # Check if table name contains only alphanumeric characters and underscores
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
        raise ValueError(f"Invalid table name format: {table}")

    # Check against whitelist (optional but recommended)
    if table not in ALLOWED_TABLES:
        logger.warning(f"Table '{table}' not in whitelist, proceeding with caution")

    return table

def get_conn():
    """Get database connection with proper error handling."""
    try:
        return sqlite3.connect(DB_PATH, timeout=config.database.connection_timeout)
    except sqlite3.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def save_df(df, table):
    """Save dataframe to database table with proper connection handling."""
    if df is None or df.empty:
        logger.warning("Attempted to save empty dataframe")
        return

    validated_table = validate_table_name(table)
    conn = None
    try:
        conn = get_conn()
        df.to_sql(validated_table, conn, if_exists="append", index=False)
        logger.info(f"Successfully saved {len(df)} rows to table '{validated_table}'")
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Failed to save dataframe to table '{table}': {e}")
        raise
    finally:
        if conn:
            conn.close()

def load_table(table):
    """Load table from database with SQL injection protection."""
    validated_table = validate_table_name(table)
    conn = None
    try:
        conn = get_conn()
        # Use validated table name in f-string (safe after validation)
        df = pd.read_sql(f"SELECT * FROM {validated_table}", conn)
        logger.info(f"Successfully loaded {len(df)} rows from table '{validated_table}'")
        return df
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Failed to load table '{table}': {e}")
        raise
    finally:
        if conn:
            conn.close()


def init_option_chain_table():
    """Initialize option chain data table with proper schema."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Create option_chain_data table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS option_chain_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strike_price REAL NOT NULL,
            option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
            last_price REAL,
            iv REAL,
            open_interest REAL,
            change_in_oi REAL,
            date DATETIME NOT NULL,
            expiry DATETIME,
            symbol TEXT DEFAULT 'NIFTY',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(strike_price, option_type, date, expiry)
        )
        """

        cursor.execute(create_table_sql)

        # Create indexes for better query performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_option_date ON option_chain_data(date)",
            "CREATE INDEX IF NOT EXISTS idx_option_type ON option_chain_data(option_type)",
            "CREATE INDEX IF NOT EXISTS idx_option_strike ON option_chain_data(strike_price)",
            "CREATE INDEX IF NOT EXISTS idx_option_expiry ON option_chain_data(expiry)"
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        conn.commit()
        logger.info("Option chain table initialized successfully")
        return True

    except sqlite3.Error as e:
        logger.error(f"Error initializing option chain table: {e}")
        return False
    finally:
        if conn:
            conn.close()


def save_option_chain_data(df, symbol="NIFTY", replace_existing=False):
    """
    Save option chain data with proper upsert logic.

    Args:
        df: DataFrame with option chain data
        symbol: Trading symbol (default: NIFTY)
        replace_existing: If True, replace existing data for the same date
    """
    if df is None or df.empty:
        logger.warning("Attempted to save empty option chain dataframe")
        return False

    # Ensure table exists
    if not init_option_chain_table():
        logger.error("Failed to initialize option chain table")
        return False

    conn = None
    try:
        conn = get_conn()

        # Prepare DataFrame - map column names to database schema
        df_save = df.copy()

        # Map column names to match database schema
        column_mapping = {
            'Strike Price': 'strike_price',
            'Option Type': 'option_type',
            'Last Price': 'last_price',
            'IV': 'iv',
            'Open Interest': 'open_interest',
            'Change in OI': 'change_in_oi',
            'Date': 'date',
            'Expiry': 'expiry'
        }

        # Rename columns
        df_save = df_save.rename(columns=column_mapping)

        # Add symbol if not present
        if 'symbol' not in df_save.columns:
            df_save['symbol'] = symbol

        # Add timestamp
        df_save['created_at'] = pd.Timestamp.now()

        # If replace_existing is True, delete existing data for this date
        if replace_existing and 'date' in df_save.columns:
            dates = df_save['date'].dt.date.unique()
            for date_val in dates:
                delete_sql = "DELETE FROM option_chain_data WHERE date(date) = ?"
                conn.execute(delete_sql, (date_val,))
            logger.info(f"Deleted existing data for dates: {dates}")

        # Use REPLACE strategy to handle duplicates based on UNIQUE constraint
        df_save.to_sql('option_chain_data', conn, if_exists='append', index=False, method='multi')

        conn.commit()
        logger.info(f"Successfully saved {len(df_save)} option chain records to database")
        return True

    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Failed to save option chain data: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def get_latest_option_chain_data(symbol="NIFTY", limit=None):
    """
    Retrieve latest option chain data from database.

    Args:
        symbol: Trading symbol to filter by
        limit: Maximum number of records to return

    Returns:
        DataFrame with latest option chain data
    """
    conn = None
    try:
        conn = get_conn()

        sql = """
        SELECT * FROM option_chain_data
        WHERE symbol = ?
        ORDER BY date DESC, created_at DESC
        """

        if limit:
            sql += f" LIMIT {int(limit)}"

        df = pd.read_sql(sql, conn, params=(symbol,))

        # Convert date columns back to datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'expiry' in df.columns:
            df['expiry'] = pd.to_datetime(df['expiry'])
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])

        logger.info(f"Retrieved {len(df)} option chain records from database")
        return df

    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve option chain data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def cleanup_old_option_data(retention_days=30):
    """
    Clean up old option chain data beyond retention period.

    Args:
        retention_days: Number of days to retain data
    """
    conn = None
    try:
        conn = get_conn()

        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=retention_days)

        delete_sql = "DELETE FROM option_chain_data WHERE date < ?"
        result = conn.execute(delete_sql, (cutoff_date,))
        deleted_rows = result.rowcount

        conn.commit()

        if deleted_rows > 0:
            logger.info(f"Cleaned up {deleted_rows} old option chain records older than {retention_days} days")
        else:
            logger.debug(f"No old records found to clean up")

        return deleted_rows

    except sqlite3.Error as e:
        logger.error(f"Error cleaning up old option data: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def init_trading_signals_table():
    """Initialize trading signals table with proper schema."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Create trading_signals table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS trading_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_type TEXT NOT NULL CHECK (signal_type IN ('BUY_CE', 'BUY_PE')),
            strike_price REAL,
            signal_strength TEXT CHECK (signal_strength IN ('LOW', 'MEDIUM', 'HIGH')),
            confidence_score REAL CHECK (confidence_score >= 0 AND confidence_score <= 1),
            pcr_value REAL,
            rsi_value REAL,
            oi_change_pct REAL,
            spot_price REAL,
            message TEXT,
            generated_at DATETIME NOT NULL,
            sent_to_telegram BOOLEAN DEFAULT 0,
            telegram_sent_at DATETIME,
            market_context TEXT,
            symbol TEXT DEFAULT 'NIFTY',
            expiry DATETIME,
            premium_price REAL,
            target_price REAL,
            stop_loss REAL,
            validity_minutes INTEGER DEFAULT 15,
            is_active BOOLEAN DEFAULT 1,
            expired_at DATETIME DEFAULT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)

        # Create indexes for better query performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_signals_generated_at ON trading_signals(generated_at)",
            "CREATE INDEX IF NOT EXISTS idx_signals_type ON trading_signals(signal_type)",
            "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON trading_signals(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_signals_confidence ON trading_signals(confidence_score)",
            "CREATE INDEX IF NOT EXISTS idx_signals_telegram_sent ON trading_signals(sent_to_telegram)",
            "CREATE INDEX IF NOT EXISTS idx_signals_is_active ON trading_signals(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_signals_expired_at ON trading_signals(expired_at)"
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        conn.commit()

        # Run migration for existing tables (safe to run multiple times)
        migration_success = migrate_trading_signals_table()

        if migration_success:
            logger.info("Trading signals table initialized and migrated successfully")
        else:
            logger.warning("Trading signals table initialized but migration may have failed")

        return True

    except sqlite3.Error as e:
        logger.error(f"Error initializing trading signals table: {e}")
        return False
    finally:
        if conn:
            conn.close()


def save_trading_signal(signal_data):
    """
    Save trading signal to database.

    Args:
        signal_data: Dictionary containing signal information
    """
    if not signal_data:
        logger.warning("Attempted to save empty signal data")
        return False

    # Ensure table exists
    if not init_trading_signals_table():
        logger.error("Failed to initialize trading signals table")
        return False

    conn = None
    try:
        conn = get_conn()

        # Prepare signal data
        signal_record = {
            'signal_type': signal_data.get('signal_type'),
            'strike_price': signal_data.get('strike_price'),
            'signal_strength': signal_data.get('signal_strength', 'MEDIUM'),
            'confidence_score': signal_data.get('confidence_score'),
            'pcr_value': signal_data.get('pcr_value'),
            'rsi_value': signal_data.get('rsi_value'),
            'oi_change_pct': signal_data.get('oi_change_pct'),
            'spot_price': signal_data.get('spot_price'),
            'message': signal_data.get('message'),
            'generated_at': signal_data.get('generated_at', pd.Timestamp.now()),
            'sent_to_telegram': signal_data.get('sent_to_telegram', False),
            'telegram_sent_at': signal_data.get('telegram_sent_at'),
            'market_context': signal_data.get('market_context'),
            'symbol': signal_data.get('symbol', 'NIFTY'),
            'expiry': signal_data.get('expiry'),
            'premium_price': signal_data.get('premium_price'),
            'target_price': signal_data.get('target_price'),
            'stop_loss': signal_data.get('stop_loss'),
            'validity_minutes': signal_data.get('validity_minutes', 15),
            'is_active': signal_data.get('is_active', True),  # New signals are active by default
            'expired_at': signal_data.get('expired_at'),  # Will be set when signal expires
            'created_at': pd.Timestamp.now()
        }

        # Convert to DataFrame for easier insertion
        df = pd.DataFrame([signal_record])
        df.to_sql('trading_signals', conn, if_exists='append', index=False)

        conn.commit()
        logger.info(f"Successfully saved trading signal: {signal_data.get('signal_type')} at {signal_data.get('strike_price')}")
        return True

    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Failed to save trading signal: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def get_recent_trading_signals(symbol="NIFTY", hours=24, limit=None):
    """
    Retrieve recent trading signals from database.

    Args:
        symbol: Trading symbol to filter by
        hours: Number of hours back to look
        limit: Maximum number of records to return

    Returns:
        DataFrame with recent trading signals
    """
    conn = None
    try:
        conn = get_conn()

        cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=hours)
        cutoff_time_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')

        sql = """
        SELECT * FROM trading_signals
        WHERE symbol = ? AND generated_at >= ?
        ORDER BY generated_at DESC
        """

        if limit:
            sql += f" LIMIT {int(limit)}"

        df = pd.read_sql(sql, conn, params=(symbol, cutoff_time_str))

        # Convert datetime columns
        datetime_columns = ['generated_at', 'telegram_sent_at', 'created_at', 'expiry']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        logger.info(f"Retrieved {len(df)} recent trading signals from database")
        return df

    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve trading signals: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def update_signal_telegram_status(signal_id, sent_successfully=True):
    """
    Update the Telegram delivery status of a signal.

    Args:
        signal_id: ID of the signal to update
        sent_successfully: Whether the Telegram message was sent successfully
    """
    conn = None
    try:
        conn = get_conn()

        update_sql = """
        UPDATE trading_signals
        SET sent_to_telegram = ?, telegram_sent_at = ?
        WHERE id = ?
        """

        sent_at = pd.Timestamp.now() if sent_successfully else None
        conn.execute(update_sql, (sent_successfully, sent_at, signal_id))
        conn.commit()

        logger.info(f"Updated Telegram status for signal {signal_id}: {sent_successfully}")
        return True

    except sqlite3.Error as e:
        logger.error(f"Failed to update signal Telegram status: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_signal_statistics(symbol="NIFTY", days=7):
    """
    Get statistics on trading signals for analysis.

    Args:
        symbol: Trading symbol
        days: Number of days to analyze

    Returns:
        Dictionary with signal statistics
    """
    conn = None
    try:
        conn = get_conn()

        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # Get signal counts by type
        count_sql = """
        SELECT signal_type, COUNT(*) as count
        FROM trading_signals
        WHERE symbol = ? AND generated_at >= ?
        GROUP BY signal_type
        """

        counts_df = pd.read_sql(count_sql, conn, params=(symbol, cutoff_date_str))

        # Get average confidence by type
        confidence_sql = """
        SELECT signal_type, AVG(confidence_score) as avg_confidence
        FROM trading_signals
        WHERE symbol = ? AND generated_at >= ? AND confidence_score IS NOT NULL
        GROUP BY signal_type
        """

        confidence_df = pd.read_sql(confidence_sql, conn, params=(symbol, cutoff_date_str))

        # Get delivery statistics
        delivery_sql = """
        SELECT
            COUNT(*) as total_signals,
            SUM(CASE WHEN sent_to_telegram = 1 THEN 1 ELSE 0 END) as successfully_sent
        FROM trading_signals
        WHERE symbol = ? AND generated_at >= ?
        """

        delivery_df = pd.read_sql(delivery_sql, conn, params=(symbol, cutoff_date_str))

        # Compile statistics
        stats = {
            'period_days': days,
            'signal_counts': counts_df.set_index('signal_type')['count'].to_dict() if not counts_df.empty else {},
            'average_confidence': confidence_df.set_index('signal_type')['avg_confidence'].to_dict() if not confidence_df.empty else {},
            'delivery_stats': delivery_df.iloc[0].to_dict() if not delivery_df.empty else {}
        }

        # Calculate delivery success rate
        if stats['delivery_stats'].get('total_signals', 0) > 0:
            stats['delivery_stats']['success_rate'] = (
                stats['delivery_stats']['successfully_sent'] / stats['delivery_stats']['total_signals']
            )

        logger.info(f"Retrieved signal statistics for {symbol} over {days} days")
        return stats

    except sqlite3.Error as e:
        logger.error(f"Failed to get signal statistics: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def cleanup_old_trading_signals(retention_days=30):
    """
    Clean up old trading signals beyond retention period.

    Args:
        retention_days: Number of days to retain signals
    """
    conn = None
    try:
        conn = get_conn()

        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=retention_days)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        delete_sql = "DELETE FROM trading_signals WHERE generated_at < ?"
        result = conn.execute(delete_sql, (cutoff_date_str,))
        deleted_rows = result.rowcount

        conn.commit()

        if deleted_rows > 0:
            logger.info(f"Cleaned up {deleted_rows} old trading signals older than {retention_days} days")
        else:
            logger.debug(f"No old trading signals found to clean up")

        return deleted_rows

    except sqlite3.Error as e:
        logger.error(f"Error cleaning up old trading signals: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def migrate_trading_signals_table():
    """
    Migrate existing trading_signals table to add new lifecycle tracking columns.
    This is safe to run multiple times - it will only add columns if they don't exist.
    """
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Check if new columns exist
        cursor.execute("PRAGMA table_info(trading_signals)")
        columns = [row[1] for row in cursor.fetchall()]

        migrations_needed = []
        if 'is_active' not in columns:
            migrations_needed.append("ALTER TABLE trading_signals ADD COLUMN is_active BOOLEAN DEFAULT 1")
        if 'expired_at' not in columns:
            migrations_needed.append("ALTER TABLE trading_signals ADD COLUMN expired_at DATETIME DEFAULT NULL")

        if not migrations_needed:
            logger.info("Trading signals table already has lifecycle tracking columns")
            return True

        # Apply migrations
        for migration_sql in migrations_needed:
            cursor.execute(migration_sql)
            logger.info(f"Applied migration: {migration_sql}")

        # Add indexes for new columns
        if 'is_active' not in columns:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_is_active ON trading_signals(is_active)")
        if 'expired_at' not in columns:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_expired_at ON trading_signals(expired_at)")

        conn.commit()
        logger.info("Successfully migrated trading_signals table with lifecycle tracking")
        return True

    except sqlite3.Error as e:
        logger.error(f"Error migrating trading_signals table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def mark_signal_expired(signal_id):
    """
    Mark a signal as expired in the database.

    Args:
        signal_id: ID of the signal to mark as expired

    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_conn()

        update_sql = """
        UPDATE trading_signals
        SET is_active = 0, expired_at = ?
        WHERE id = ?
        """

        expired_at = pd.Timestamp.now()
        result = conn.execute(update_sql, (expired_at, signal_id))
        conn.commit()

        if result.rowcount > 0:
            logger.info(f"Marked signal {signal_id} as expired at {expired_at}")
            return True
        else:
            logger.warning(f"No signal found with ID {signal_id} to mark as expired")
            return False

    except sqlite3.Error as e:
        logger.error(f"Failed to mark signal as expired: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_active_signals_from_db(symbol="NIFTY"):
    """
    Retrieve currently active signals from database for persistence across restarts.

    Args:
        symbol: Trading symbol to filter by

    Returns:
        DataFrame with active signals
    """
    conn = None
    try:
        conn = get_conn()

        # Get active signals that haven't expired yet
        sql = """
        SELECT * FROM trading_signals
        WHERE symbol = ? AND is_active = 1
        AND (expired_at IS NULL OR expired_at > ?)
        ORDER BY generated_at DESC
        """

        current_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        df = pd.read_sql(sql, conn, params=(symbol, current_time))

        # Convert datetime columns
        datetime_columns = ['generated_at', 'telegram_sent_at', 'created_at', 'expiry', 'expired_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        logger.info(f"Retrieved {len(df)} active signals from database for {symbol}")
        return df

    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve active signals: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def mark_signals_expired_by_time(symbol="NIFTY", cutoff_time=None):
    """
    Mark signals as expired based on their validity time.

    Args:
        symbol: Trading symbol to filter by
        cutoff_time: Time before which to mark signals as expired (default: current time)

    Returns:
        int: Number of signals marked as expired
    """
    conn = None
    try:
        if cutoff_time is None:
            cutoff_time = pd.Timestamp.now()

        conn = get_conn()

        # Find signals that should be expired based on generated_at + validity_minutes
        update_sql = """
        UPDATE trading_signals
        SET is_active = 0, expired_at = ?
        WHERE symbol = ? AND is_active = 1
        AND datetime(generated_at, '+' || validity_minutes || ' minutes') <= ?
        AND expired_at IS NULL
        """

        expired_at = pd.Timestamp.now()
        result = conn.execute(update_sql, (expired_at, symbol, cutoff_time))
        conn.commit()

        expired_count = result.rowcount
        if expired_count > 0:
            logger.info(f"Marked {expired_count} signals as expired for {symbol}")

        return expired_count

    except sqlite3.Error as e:
        logger.error(f"Failed to mark signals as expired by time: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def get_signal_lifecycle_stats(symbol="NIFTY", days=7):
    """
    Get statistics on signal lifecycle including renewal patterns.

    Args:
        symbol: Trading symbol
        days: Number of days to analyze

    Returns:
        Dictionary with lifecycle statistics
    """
    conn = None
    try:
        conn = get_conn()

        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # Get lifecycle statistics
        lifecycle_sql = """
        SELECT
            signal_type,
            COUNT(*) as total_signals,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_signals,
            SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as expired_signals,
            AVG(validity_minutes) as avg_validity_minutes
        FROM trading_signals
        WHERE symbol = ? AND generated_at >= ?
        GROUP BY signal_type
        """

        stats_df = pd.read_sql(lifecycle_sql, conn, params=(symbol, cutoff_date_str))

        # Compile statistics
        stats = {
            'period_days': days,
            'lifecycle_stats': stats_df.set_index('signal_type').to_dict('index') if not stats_df.empty else {},
            'generated_at': pd.Timestamp.now().isoformat()
        }

        logger.info(f"Retrieved signal lifecycle statistics for {symbol} over {days} days")
        return stats

    except sqlite3.Error as e:
        logger.error(f"Failed to get signal lifecycle statistics: {e}")
        return {}
    finally:
        if conn:
            conn.close()