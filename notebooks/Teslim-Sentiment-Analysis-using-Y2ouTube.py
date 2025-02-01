import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the API key
api_key = os.getenv("API_KEY")

if not api_key:
    raise ValueError("YOUTUBE_API_KEY not loaded. Please check your .env file.")
else:
    print("API key loaded successfully.")

# Setting Up YouTube API Client
import googleapiclient.discovery
import googleapiclient.errors

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = os.getenv("api_key")

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key
)

# import pandas
import pandas as pd

# List of video IDs
video_ids = ["2F26DR3Be4g", "vAVsczZ3Oqk"]

# Fetch Comments from a Video
comments = []

for video_id in video_ids:
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100
    )
    response = request.execute()

    for item in response['items']:
        comment = item['snippet']['topLevelComment']['snippet']
        comments.append([
            video_id,  # Add video ID to identify the source video
            comment['authorDisplayName'],
            comment['publishedAt'],
            comment['updatedAt'],
            comment['likeCount'],
            comment['textDisplay']
        ])

# Create a DataFrame with an additional column for video ID
df = pd.DataFrame(comments, columns=['video_id', 'author', 'published_at', 'updated_at', 'like_count', 'text'])

# Display the DataFrame
df.head()

# check the columns
df.columns.to_list()

# setting up the MySQL connection

MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

if not MYSQL_USERNAME or not MYSQL_PASSWORD:
    raise ValueError("MYSQL_USERNAME or MYSQL_PASSWORD not loaded. Please check your .env file.")
else:
    print("MySQL credentials loaded successfully.")

# Import the MySQL connector
import mysql.connector

# Database connection
def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD
    )

# Create database if it does not exist
def create_database():
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS uk_migration_youtube_comments_db")
    conn.close()

# Connect to the database
def connect_to_db_with_db():
    return mysql.connector.connect(
        host="localhost",
        user=MYSQL_USERNAME,
        password=MYSQL_PASSWORD,
        database="uk_migration_youTube_comments_db"
    )

# Call the functions
create_database()
db = connect_to_db_with_db()

# Create table
def create_table():
    conn = connect_to_db_with_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_comments (
            video_id VARCHAR(255),
            author VARCHAR(255),
            published_at DATETIME,
            updated_at DATETIME,
            like_count INT,
            text TEXT
        )
    """)
    conn.close()

# Call the function to create the table
create_table()

# Function to insert data into the table
def insert_data(df):
    # Connect to the database
    conn = connect_to_db_with_db()
    cursor = conn.cursor()

    # Convert datetime columns to MySQL-compatible format
    df['published_at'] = pd.to_datetime(df['published_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['updated_at'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Iterate over each row in the DataFrame and insert it into the table
    for index, row in df.iterrows():
        video_id = row['video_id']
        author = row['author']
        published_at = row['published_at']
        updated_at = row['updated_at']
        like_count = row['like_count']
        text = row['text']

        # Execute the SQL query to insert the data
        cursor.execute("""
            INSERT INTO youtube_comments (video_id, author, published_at, updated_at, like_count, text)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (video_id, author, published_at, updated_at, like_count, text))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()

# Call the function to insert data
insert_data(df)

# Function to execute SQL queries and display results
def execute_query(query):
    conn = connect_to_db_with_db()
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Fetch results and convert to a DataFrame
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]  # Column names
    
    cursor.close()
    conn.close()
    
    return pd.DataFrame(result, columns=columns)


sqlquery = """

SELECT * FROM youtube_comments;

"""
# Execute the query and display the results
result_df = execute_query(sqlquery)
result_df