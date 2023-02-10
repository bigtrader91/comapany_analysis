import mysql.connector
import os
user=os.environ.get('user')
password=os.environ.get('password')
host=os.environ.get('host')

# Connect to the MySQL server
cnx = mysql.connector.connect(user=user, password=password, host=host)
cursor = cnx.cursor()

# Create the database
sql = "CREATE DATABASE IF NOT EXISTS news_db"
cursor.execute(sql)

# Use the database
sql = "USE news_db"
cursor.execute(sql)

# Create the table
sql = """CREATE TABLE IF NOT EXISTS news_naver (
            id INT AUTO_INCREMENT PRIMARY KEY,
            datetime DATETIME NOT NULL,
            category VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            text TEXT NOT NULL,
            link VARCHAR(255) NOT NULL
        )"""
cursor.execute(sql)

sql = """CREATE TABLE IF NOT EXISTS news_daum (
            id INT AUTO_INCREMENT PRIMARY KEY,
            datetime DATETIME NOT NULL,
            title VARCHAR(255) NOT NULL,
            text TEXT NOT NULL,
            link VARCHAR(255) NOT NULL
        )"""
cursor.execute(sql)

# Commit the changes
cnx.commit()

# Close the cursor and the connection
cursor.close()
cnx.close()