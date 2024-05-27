import googleapiclient.discovery
import pandas as pd
import streamlit as st
import re
import mysql.connector
import isodate


# API KEY
def Api_access():
    api_key = "AIzaSyAgnUOq1GyJdJ-fMpRo9VHWc5qOWvRrkAE"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = Api_access()

# Function to get Channel details with help of channel id
def channel_name(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    results = {
        "channel_id": channel_id,
        "channel_name": response['items'][0]['snippet']['title'],
        "channel_description": response['items'][0]['snippet']['description'],
        "channel_pubAt": response['items'][0]['snippet']['publishedAt'],
        "channel_viewscount": response['items'][0]['statistics']['viewCount'],
        "channel_Subcount": response['items'][0]['statistics']['subscriberCount'],
        "channel_playlistID": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "channel_videocount": response['items'][0]['statistics']['videoCount']
    }
    return [results]

# Function to get video ids
def video_id(channel_id):
    vid_ids = []
    request = youtube.channels().list(part="contentDetails", id=channel_id)
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(part="snippet", playlistId=playlist_id, maxResults=50, pageToken=next_page_token)
        request = response1.execute()
        for i in range(len(request['items'])):
            vid_ids.append(request['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = request.get('nextPageToken')
        if next_page_token is None:
            break
    return vid_ids

# Function to fetch video details
def get_video_data(video_id_details):
    video_data = []
    for video_list in video_id_details:
        request = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_list)
        response = request.execute()
        video_info = response['items'][0]
        Video_data = {
            "channel_name": video_info['snippet']['channelTitle'],
            "channel_id": video_info['snippet']['channelId'],
            "video_id": video_info['id'],
            "title": video_info['snippet']['title'],
            "likeCount": int(video_info['statistics'].get('likeCount', 0)),
            "thumbnail": video_info['snippet']['thumbnails']['default']['url'],
            "video_pubAt": video_info['snippet']['publishedAt'],
            "video_description": video_info['snippet']['description'],
            "duration": parse_iso_duration(video_info['contentDetails']['duration']),
            "channel_viewscount": video_info['statistics']['viewCount'],
            "comment_count":int(video_info['statistics'].get('commentCount',0)),
            "favorite_count": video_info['statistics']['favoriteCount'],
            "caption_status": video_info['contentDetails']['caption']
        }
        video_data.append(Video_data)
    return video_data

# Function to parse ISO 8601 duration
# def parse_iso_duration(duration):
#     pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
#     match = pattern.match(duration)
#     if not match:
#         raise ValueError("Invalid ISO 8601 duration format")
#     hours, minutes, seconds = match.groups(default="0")
#     return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
def parse_iso_duration(duration):
    try:
        parsed_duration = isodate.parse_duration(duration)
        # Convert parsed_duration to a string in HH:MM:SS format
        total_seconds = int(parsed_duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    except isodate.ISO8601Error:
        raise ValueError("Invalid ISO 8601 duration format")

# Function to fetch comment info
def get_comment_info(video_id_details):
    comment_lst = []
    for video_id in video_id_details:
        try:
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
            response = request.execute()
            for item in response['items']:
                data = {
                    "comment_id": item['id'],
                    "video_id": item['snippet']['topLevelComment']['snippet']['videoId'],
                    "comment_txt": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "comment_author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "comment_pub": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_lst.append(data)
        except:
            pass
    return comment_lst

# Assigning all the details in a single function
def channel_informations(channel_id):
    # Retrieve channel details
    channel_details = channel_name(channel_id)
    
    # Retrieve video ids
    video_ids = video_id(channel_id)
    
    # Retrieve video details
    video_details = get_video_data(video_ids)
    
    # Retrieve comment details
    comment_details = get_comment_info(video_ids)
    
    # Creating dataframes for channel details, video details, and comments
    df_channel = pd.DataFrame(channel_details)
    df_channel['channel_pubAt'] = pd.to_datetime(df_channel['channel_pubAt']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df_video = pd.DataFrame(video_details)
    df_video['video_pubAt'] = pd.to_datetime(df_video['video_pubAt']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df_comment = pd.DataFrame(comment_details)
    df_comment['comment_pub'] = pd.to_datetime(df_comment['comment_pub']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df_channel, df_video, df_comment

# MySQL connection and table creation


def video_table(df_video):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Shoaib@21",
        database="tabledata"
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS tabledata")
    mycursor.close()
    mydb.close()
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apple@123",
        database="tabledata"
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS VID_DETAILS (channel_name VARCHAR(255), channel_id VARCHAR(255), video_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), likeCount INT, video_pubAt DATETIME, video_description VARCHAR(255), duration VARCHAR(255), channel_viewscount INT, comment_count INT, favorite_count INT, caption_status VARCHAR(255))")
    for index, row in df_video.iterrows():
        sql = "INSERT IGNORE INTO VID_DETAILS (channel_name, channel_id, video_id, title, likeCount, video_pubAt, video_description, duration, channel_viewscount, comment_count, favorite_count, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row['channel_name'], row['channel_id'], row['video_id'], row['title'], row.get('likeCount'), row.get('video_pubAt'), row['video_description'], row['duration'], row['channel_viewscount'], row['comment_count'], row['favorite_count'], row['caption_status'])
        mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()

def channel_table(df_channel):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apple@123",
        database="tabledata"
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS channel_data (channel_id VARCHAR(80) PRIMARY KEY, channel_name VARCHAR(100), channel_description VARCHAR(255), channel_pubAt DATETIME, channel_viewscount INT, channel_Subcount INT, channel_playlistID VARCHAR(255), channel_videocount INT)")    
    for index, row in df_channel.iterrows():
        sql = "INSERT IGNORE INTO channel_data (channel_id, channel_name, channel_description, channel_pubAt, channel_viewscount, channel_Subcount, channel_playlistID, channel_videocount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row['channel_id'], row['channel_name'], row['channel_description'], row['channel_pubAt'], row['channel_viewscount'], row['channel_Subcount'], row['channel_playlistID'], row['channel_videocount'])
        mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
def comment_table(df_comment):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apple@123",
        database="tabledata"
    )
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS Details_of_Comments (comment_id VARCHAR(255) PRIMARY KEY, video_id VARCHAR(255), comment_txt TEXT, comment_author VARCHAR(255), comment_pub TIMESTAMP)")
    for index, row in df_comment.iterrows():
        sql = "INSERT IGNORE INTO Details_of_Comments (comment_id, video_id, comment_txt, comment_author, comment_pub) VALUES (%s, %s, %s, %s, %s)"
        val = (row['comment_id'], row['video_id'], row['comment_txt'], row['comment_author'], row.get('comment_pub'))
        mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()

# Function to create and insert data into tables
def tables(df_channel, df_video, df_comment):
    channel_table(df_channel)
    video_table(df_video)
    comment_table(df_comment)
    return "Tables Created Successfully"

# Streamlit
st.title("YouTube Data Harvesting")
channel_id = st.text_input("Enter the Channel ID")

if st.button("Fetch Data"):
    df_channel, df_video, df_comment = channel_informations(channel_id)
    st.write("### Channel Data")
    st.dataframe(df_channel)
    st.write("### Video Data")
    st.dataframe(df_video)
    st.write("### Comment Data")
    st.dataframe(df_comment)
    st.success("Data Collected")

if st.button("Transfer to SQL"):
    df_channel, df_video, df_comment = channel_informations(channel_id)
    result = tables(df_channel, df_video, df_comment)
    st.success(result)

# SQL Query Execution
def execute_query(query):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apple@123",
        database="tabledata"
    )
    mycursor = mydb.cursor()
    mycursor.execute(query)
    result = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    return result

Question = st.selectbox("Select your question", (
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
))

# Add the logic to execute the appropriate SQL queries based on the selected question
if Question == "1. What are the names of all the videos and their corresponding channels?":
    query = "SELECT title, channel_name FROM VID_DETAILS"
    result = execute_query(query)
    st.table(result)
elif Question == "2. Which channels have the most number of videos, and how many videos do they have?":
    query = "SELECT channel_name, COUNT(*) as video_count FROM VID_DETAILS GROUP BY channel_name ORDER BY video_count DESC"
    result = execute_query(query)
    st.table(result)

elif Question == "3. What are the top 10 most viewed videos and their respective channels?":
    query = "SELECT title, channel_name, channel_viewscount FROM VID_DETAILS ORDER BY channel_viewscount DESC LIMIT 10"
    result = execute_query(query)
    st.table(result)

elif Question == "4. How many comments were made on each video, and what are their corresponding video names?":
    query = "SELECT title, comment_count FROM VID_DETAILS"
    result = execute_query(query)
    st.table(result)

elif Question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query = "SELECT title, channel_name, likeCount FROM VID_DETAILS ORDER BY likeCount DESC"
    result = execute_query(query)
    st.table(result)

elif Question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query = "SELECT title, likeCount, favorite_count FROM VID_DETAILS"
    result = execute_query(query)
    st.table(result)

elif Question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query = "SELECT channel_name, SUM(channel_viewscount) as total_views FROM VID_DETAILS GROUP BY channel_name"
    result = execute_query(query)
    st.table(result)

elif Question == "8. What are the names of all the channels that have published videos in the year 2022?":
    query = "SELECT DISTINCT channel_name FROM VID_DETAILS WHERE YEAR(video_pubAt) = 2022"
    result = execute_query(query)
    st.table(result)

elif Question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query = "SELECT channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) as avg_duration FROM VID_DETAILS GROUP BY channel_name"
    result = execute_query(query)
    st.table(result)

elif Question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query = "SELECT title, channel_name, comment_count FROM VID_DETAILS ORDER BY comment_count DESC"
    result = execute_query(query)
    st.table(result)
