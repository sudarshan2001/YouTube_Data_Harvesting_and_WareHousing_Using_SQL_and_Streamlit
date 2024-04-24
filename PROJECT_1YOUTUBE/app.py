import streamlit as st
import mysql.connector
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import re


#Youtube API service:
api_service_name = "youtube"
api_version = "v3"
api_key="*********"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)


def fetch_data(query):
    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="youtube")
    df = pd.read_sql(query, mydb)
    mydb.close()
    return df

# QUERIES...
def execute_query(question):
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?": """
            SELECT videos.Video_title, channels.channel_name
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id;
        """,
        "Which channels have the most number of videos, and how many videos do they have?": """
            SELECT channel_name, COUNT(*) AS video_count
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            GROUP BY channel_name
            ORDER BY video_count DESC
            LIMIT 1;
        """,
        "What are the top 10 most viewed videos and their respective channels?": """
            SELECT videos.Video_title, channels.channel_name
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            ORDER BY videos.Video_viewcount DESC
            LIMIT 10;
        """,
        "How many comments were made on each video, and what are their corresponding video names?": """
            SELECT videos.Video_title, COUNT(*) AS comment_count
            FROM videos
            JOIN comments ON videos.Video_Id = comments.video_id
            GROUP BY videos.Video_title;
        """,
        "Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT videos.Video_title, channels.channel_name
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            ORDER BY videos.Video_likecount DESC
            LIMIT 1;
        """,
        "What is the total number of likes for each video, and what are their corresponding video names?": """
            SELECT videos.Video_title, SUM(videos.Video_likecount) AS total_likes
            FROM videos
            GROUP BY videos.Video_title;
        """,
        "What is the total number of views for each channel, and what are their corresponding channel names?": """
            SELECT channels.channel_name, SUM(videos.Video_viewcount) AS total_views
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            GROUP BY channels.channel_name;
        """,
        "What are the names of all the channels that have published videos in the year 2022?": """
            SELECT DISTINCT channels.channel_name
            FROM channels
            JOIN videos ON channels.channel_id = videos.channel_id
            WHERE YEAR(videos.Video_pubdate) = 2022;
        """,
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            SELECT channels.channel_name, AVG(videos.Video_duration) AS average_duration
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            GROUP BY channels.channel_name;
        """,
        "Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT videos.Video_title, channels.channel_name
            FROM videos
            JOIN channels ON videos.channel_id = channels.channel_id
            ORDER BY videos.Video_commentcount DESC
            LIMIT 1;
        """
    }

    query = query_mapping.get(question)
    if query:
        return fetch_data(query)
    else:
        return pd.DataFrame()
    

# fetch channel data:
def fetch_channel_data(channel_id):
    request = youtube.channels().list(
            part="contentDetails,snippet,statistics",
            id= channel_id
        )
    response = request.execute()

    data={ 'channel_id':channel_id,
            'channel_name':response['items'][0]['snippet']['title'], 
            'channel_des':response['items'][0]['snippet']['description'],
            'channel_playid':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_vidcount':response['items'][0]['statistics']['videoCount'],
            'channel_viewcount':response['items'][0]['statistics']['viewCount'],
            'channel_subcount':response['items'][0]['statistics']['subscriberCount']}
    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="youtube")
    cursor = mydb.cursor()
    cursor.execute("""
                INSERT INTO channels (channel_id, channel_name, channel_des, channel_playid, channel_vidcount, channel_viewcount, channel_subcount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (data['channel_id'], data['channel_name'], data['channel_des'], data['channel_playid'], data['channel_vidcount'], data['channel_viewcount'], data['channel_subcount']))
    mydb.commit()
    mydb.close()
    return pd.DataFrame(data, index=[0])

# fetch videos from channel:
def fetch_videos_from_channel(channel_id):
    video_ids = []

    try:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=50 
        )
        response = request.execute()

        for item in response['items']:
            if item['id']['kind'] == 'youtube#video':
                video_ids.append(item['id']['videoId'])

    except HttpError as e:
        print(f"HTTP Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return video_ids

    
# fetch video data:
def fetch_video_data(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                part="contentDetails,snippet,statistics",
                id=video_id
            )
        response = request.execute()

        for details in response["items"]:
            caption_available = details['contentDetails'].get('caption', False)
            data= {
                'Video_Id': details['id'],
                'Video_title': details['snippet']['title'],
                'channel_id': details['snippet']['channelId'],
                'Video_Description': details['snippet']['description'],
                'Video_pubdate': details['snippet']['publishedAt'],
                'Video_thumbnails': details['snippet']['thumbnails']['default']['url'],
                'Video_viewcount': details['statistics']['viewCount'],
                'Video_likecount': details['statistics'].get('likeCount', 0),
                'Video_favoritecount': details['statistics']['favoriteCount'],
                'Video_commentcount': details['statistics'].get('commentCount', 0),
                'Video_duration': iso8601_duration_to_seconds(details['contentDetails']['duration']),
                'Video_caption': caption_available
            }

            video_data.append(data)

    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="youtube")
    cursor = mydb.cursor()
    for video in video_data:
        cursor.execute("""
            INSERT INTO videos (Video_Id, Video_title, channel_id, Video_Description, Video_pubdate, Video_thumbnails, Video_viewcount, Video_likecount, Video_favoritecount, Video_commentcount, Video_duration, Video_caption)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (video['Video_Id'], video['Video_title'], video['channel_id'], video['Video_Description'], video['Video_pubdate'], video['Video_thumbnails'], video['Video_viewcount'], video['Video_likecount'], video['Video_favoritecount'], video['Video_commentcount'], video['Video_duration'], video['Video_caption']))
    mydb.commit()
    mydb.close()

    return pd.DataFrame(video_data)

# fetch comment data:
def fetch_comment_data(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            next_page_token = None
            while True:
                try:
                    request_comments = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=next_page_token)
                    response_comments = request_comments.execute()

                    for comment in response_comments["items"]:
                        data = {
                            'comment_id': comment['snippet']['topLevelComment']['id'],
                            'video_id': comment['snippet']['topLevelComment']['snippet']['videoId'],
                            'channel_id': comment['snippet']['channelId'],
                            'author_name': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'text_display': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'published_date': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        comment_data.append(data)

                    next_page_token = response_comments.get('nextPageToken')

                    if next_page_token is None:
                        break
                except HttpError as e:
                    if e.resp.status == 404:
                        print(f"Comments are disabled for video ID: {video_id}")
                        break
                    else:
                        raise
    except Exception as e:
        print(f"An error occurred: {e}")

    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="youtube")
    cursor = mydb.cursor()
    for comment in comment_data:
        cursor.execute("""
            INSERT INTO comments (comment_id, video_id, channel_id, author_name, text_display, published_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (comment['comment_id'], comment['video_id'], comment['channel_id'], comment['author_name'], comment['text_display'], comment['published_date']))
    mydb.commit()
    mydb.close()

    return pd.DataFrame(comment_data)

def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds


# delete the channel data:
def delete_channel_data(channel_id):
    mydb = mysql.connector.connect(host="localhost", user="root", password="", database="youtube")
    cursor = mydb.cursor()
    try:
        query_delete_comments = f"DELETE FROM comments WHERE video_id IN (SELECT Video_Id FROM videos WHERE channel_id = '{channel_id}')"
        cursor.execute(query_delete_comments)
        
        query_delete_videos = f"DELETE FROM videos WHERE channel_id = '{channel_id}'"
        cursor.execute(query_delete_videos)
        
        query_delete_channel = f"DELETE FROM channels WHERE channel_id = '{channel_id}'"
        cursor.execute(query_delete_channel)

        mydb.commit()

        deletion_success = True
    except Exception as e:
        mydb.rollback()
        print("Error:", e)
        deletion_success = False
    finally:
        cursor.close()
        mydb.close()
    return deletion_success


def main():
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")

    selected_option = st.sidebar.radio("Select Option", ("Channels", "Videos", "Comments", "Queries", "Enter YouTube Channel ID"))
    
    
    if selected_option == "Channels":
        st.header("Channels")
        channels_df = fetch_data("SELECT * FROM channels;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif selected_option == "Videos":
        st.header("Videos")
        videos_df = fetch_data("SELECT * FROM videos;")
        videos_df.index += 1
        st.dataframe(videos_df)


    elif selected_option == "Comments":
        st.header("Comments")
        comments_df = fetch_data("SELECT * FROM comments;")
        comments_df.index += 1
        st.dataframe(comments_df)



    elif selected_option == "Queries":
        st.header("Queries")
        query_question = st.selectbox("Select Query", [
            "What are the names of all the videos and their corresponding channels?",
            "Which channels have the most number of videos, and how many videos do they have?",
            "What are the top 10 most viewed videos and their respective channels?",
            "How many comments were made on each video, and what are their corresponding video names?",
            "Which videos have the highest number of likes, and what are their corresponding channel names?",
            "What is the total number of likes for each video, and what are their corresponding video names?",
            "What is the total number of views for each channel, and what are their corresponding channel names?",
            "What are the names of all the channels that have published videos in the year 2022?",
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "Which videos have the highest number of comments, and what are their corresponding channel names?"
        ])
        if query_question:
            query_result_df = execute_query(query_question)
            query_result_df.index += 1
            st.dataframe(query_result_df)

    elif selected_option == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")

        if st.button("Fetch Channel Data"):
            channel_df = fetch_channel_data(channel_id)
            channel_df.index += 1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch video Data"):
            video_ids = fetch_videos_from_channel(channel_id)
            video_df = fetch_video_data(video_ids)
            video_df.index += 1
            st.subheader("video Data")
            st.write(video_df)

        if st.button("Fetch Comment Data"):
            video_ids = fetch_videos_from_channel(channel_id)
            comment_df = fetch_comment_data(video_ids)
            comment_df.index += 1
            st.subheader("Comment Data")
            st.write(comment_df)

        if st.button("Delete Channel"):
                deleted_successfully = delete_channel_data(channel_id)
                if deleted_successfully:
                    st.success("Channel and associated data deleted successfully.")
                else:
                    st.error("Failed to delete channel and associated data.")

if __name__ == "__main__":
    main()

