"""
Simple test script to verify the options data collection system
"""
from os import path 
import sys
from datetime import datetime
from sqlalchemy import text
import logging

# Add current directory to path
sys.path.append(path.dirname(path.abspath(__file__)))

def test_imports():
    """Test if all required modules can be imported"""
    print("Testing imports...")
    
    try:
        from database import DatabaseManager, OptionsData, UnderlyingPriceData
        print("✓ Database modules imported successfully")
    except Exception as e:
        print(f"✗ Database import failed: {e}")
        return False
    
    try:
        from options_calculator import OptionsCalculator
        print("✓ Options calculator imported successfully")
    except Exception as e:
        print(f"✗ Options calculator import failed: {e}")
        return False
    
    try:
        from skew_data_collector import OptionsSkewDataCollector
        print("✓ Enhanced collector imported successfully")
    except Exception as e:
        print(f"✗ Enhanced collector import failed: {e}")
        return False
    
    try:
        import option_greeks_update
        print("✓ Main module imported successfully")
    except Exception as e:
        print(f"✗ Main module import failed: {e}")
        return False
    
    return True

def test_calculator():
    """Test the options calculator"""
    print("\nTesting options calculator...")
    
    try:
        from options_calculator import OptionsCalculator
        
        calc = OptionsCalculator()
        
        # Test Black-Scholes pricing
        S, K, T, r, sigma = 100, 100, 0.25, 0.05, 0.2
        call_price = calc.black_scholes_price(S, K, T, r, sigma, 'C')
        put_price = calc.black_scholes_price(S, K, T, r, sigma, 'P')
        
        print(f"✓ Black-Scholes pricing: Call=${call_price:.2f}, Put=${put_price:.2f}")
        
        # Test Greeks
        delta = calc.calculate_delta(S, K, T, r, sigma, 'C')
        gamma = calc.calculate_gamma(S, K, T, r, sigma)
        
        print(f"✓ Greeks calculation: Delta={delta:.3f}, Gamma={gamma:.4f}")
        
        # Test IV calculation  
        iv = calc.calculate_implied_volatility(call_price, S, K, T, r, 'C')
        print(f"✓ Implied volatility: {iv:.3f} (should be close to {sigma})")
        
        # Test moneyness and ATM
        moneyness = calc.calculate_moneyness(K, S)
        is_atm = calc.is_at_the_money(K, S)
        print(f"✓ Moneyness: {moneyness:.3f}, ATM: {is_atm}")
        
        return True
        
    except Exception as e:
        print(f"✗ Calculator test failed: {e}")
        return False

def test_database_connection():
    """Test database connection (if available)"""
    print("\nTesting database connection...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        from database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test connection
        with db_manager.get_session() as session:
            result = session.execute(text("""SELECT 1""")).fetchone()
            
            # result = False
            if result:
                print("✓ Database connection successful")
                
                # Test table creation
                db_manager.create_tables()
                print("✓ Database tables created/verified")
                
                db_manager.close()
                return True
                
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        print("  Note: This is expected if PostgreSQL is not configured")
        return False

def main():
    """Run all tests"""
    print("Options Data Manager System Test")
    print("=" * 40)
    
    # Set up basic logging
    logging.basicConfig(level=logging.WARNING)
    
    tests_passed = 0
    total_tests = 3
    
    # Test imports
    if test_imports():
        tests_passed += 1
    
    # Test calculator
    if test_calculator():
        tests_passed += 1
    
    # Test database (optional)
    if test_database_connection():
        tests_passed += 1
    
    print("\n" + "=" * 40)
    print(f"Tests completed: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("✓ All tests passed! System is ready.")
        return True
    elif tests_passed >= 2:
        print("⚠ Core functionality working. Check database configuration.")
        return True
    else:
        print("✗ System has issues. Check dependencies and configuration.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)