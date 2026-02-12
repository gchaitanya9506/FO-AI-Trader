#!/usr/bin/env python3
"""
Signal Expiry and Renewal System Test
Tests the complete signal lifecycle with expiry tracking and renewal.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from strategy.advanced_signal_engine import get_signal_engine
from database.db_manager import get_active_signals_from_db, mark_signal_expired
from datetime import datetime, timedelta
import time
import json

def simulate_signal_generation():
    """Simulate signal generation by creating test signals."""
    print("🧪 TESTING SIGNAL EXPIRY AND RENEWAL SYSTEM")
    print("=" * 60)

    # Get engine
    engine = get_signal_engine()

    print("\n1. INITIAL STATE:")
    status = engine.get_signal_status()
    print(f"   Market Hours: {status.get('market_hours')}")
    print(f"   Signal Renewal Enabled: {status.get('signal_renewal_enabled')}")
    print(f"   Active Signals: {status.get('active_signals_count')}")

    print("\n2. TESTING MANUAL SIGNAL TRACKING:")

    # Create test signals
    test_ce_signal = {
        'signal_type': 'BUY_CE',
        'strike_price': 21000,
        'signal_strength': 'HIGH',
        'confidence_score': 0.85,
        'pcr_value': 0.65,
        'rsi_value': 42,
        'spot_price': 20980,
        'generated_at': datetime.now(),
        'validity_minutes': 1,  # 1 minute for quick testing
        'symbol': 'NIFTY'
    }

    test_pe_signal = {
        'signal_type': 'BUY_PE',
        'strike_price': 20950,
        'signal_strength': 'MEDIUM',
        'confidence_score': 0.75,
        'pcr_value': 1.35,
        'rsi_value': 68,
        'spot_price': 20980,
        'generated_at': datetime.now() + timedelta(seconds=10),  # Slightly later
        'validity_minutes': 1,  # 1 minute for quick testing
        'symbol': 'NIFTY'
    }

    # Track signals manually
    print(f"   Tracking CE signal: {test_ce_signal['signal_type']} at {test_ce_signal['strike_price']}")
    engine.track_active_signal(test_ce_signal)

    print(f"   Tracking PE signal: {test_pe_signal['signal_type']} at {test_pe_signal['strike_price']}")
    engine.track_active_signal(test_pe_signal)

    # Check active signals
    active_signals = engine.get_active_signals()
    print(f"   Active signals after tracking: {list(active_signals.keys())}")

    print("\n3. TESTING SIGNAL ACTIVITY CHECKS:")
    print(f"   BUY_CE is_active: {engine.is_signal_active('BUY_CE')}")
    print(f"   BUY_PE is_active: {engine.is_signal_active('BUY_PE')}")
    print(f"   BUY_CE is_expired: {engine.is_signal_expired('BUY_CE')}")
    print(f"   BUY_PE is_expired: {engine.is_signal_expired('BUY_PE')}")

    print(f"\n4. WAITING FOR SIGNAL EXPIRY (65 seconds)...")
    print("   ⏳ Waiting for signals to expire...")

    # Wait for expiry (1 minute + buffer)
    for i in range(13):  # 65 seconds total
        time.sleep(5)
        if i % 2 == 0:  # Every 10 seconds
            ce_active = engine.is_signal_active('BUY_CE')
            pe_active = engine.is_signal_active('BUY_PE')
            print(f"   {i*5+5}s: CE_active={ce_active}, PE_active={pe_active}")

    print(f"\n5. POST-EXPIRY STATE:")
    print(f"   BUY_CE is_active: {engine.is_signal_active('BUY_CE')}")
    print(f"   BUY_PE is_active: {engine.is_signal_active('BUY_PE')}")
    print(f"   BUY_CE is_expired: {engine.is_signal_expired('BUY_CE')}")
    print(f"   BUY_PE is_expired: {engine.is_signal_expired('BUY_PE')}")

    # Test expiry cleanup
    print(f"\n6. EXPIRY CLEANUP:")
    print("   Running clear_expired_signals()...")
    engine.clear_expired_signals()

    active_after_cleanup = engine.get_active_signals()
    print(f"   Active signals after cleanup: {list(active_after_cleanup.keys())}")

    print(f"\n7. FINAL ENGINE STATUS:")
    final_status = engine.get_signal_status()
    print(f"   Active Signals Count: {final_status.get('active_signals_count')}")
    print(f"   Recent Signals Count: {final_status.get('recent_signals_count')}")

    print("\n✅ SIGNAL EXPIRY TESTING COMPLETED!")
    print("=" * 60)

if __name__ == "__main__":
    try:
        simulate_signal_generation()
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()