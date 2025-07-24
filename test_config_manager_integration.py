# test_new_config.py
from src.configuration_manager import ConfigurationManager

def test_config_manager():
    try:
        config = ConfigurationManager()
        
        print(f"✅ Project: {config.project_name}")
        print(f"✅ Environment: {config.get_environment()}")
        print(f"✅ Database enabled: {config.is_database_enabled}")
        print(f"✅ Data mode: {config.get_data_mode()}")
        
        if config.is_database_enabled:
            print(f"✅ DB Host: {config.database.host}")
        
        print("🎉 Configuration Manager working perfectly!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_config_manager()