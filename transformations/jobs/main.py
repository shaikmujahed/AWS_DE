import os
import sys
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from resources.dev import config
from src.main.utility.mysql_session import *
from src.main.utility.loggin_config import *



csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]

connection = get_mysql_connection()
cursor = connection.cursor()
if csv_files:
    formatted_files = ", ".join(f"'{file}'" for file in csv_files)
    print('formatted_files:->>>>>> ',formatted_files)

    statement = f"""
        SELECT DISTINCT file_name FROM {config.db_name}.{config.table_name}
        WHERE file_name IN ({formatted_files}) AND status='I'
    """
    logger.info(f"dynamically statement created: {statement} ")

    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("your last run was failed please look into it once...")
    else:
        logger.info('No records match.')
else:
    logger.info("last run was successful!")


