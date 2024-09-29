#Import necessary libraries
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import pandas as pd
import mysql.connector 
import dotenv
from dateutil import parser
import re


#load environment variables from the .env file
dotenv.load_dotenv()

#Access Environment variables using os.gentenv() methods
HOST_NAME = 'localhost'
USER_NAME = 'root'
PASS = 'aashi'


api_key ='AIzaSyCCSZxuJKwkCy1DVsUS5e6fqp00jiTOfpA'
youtube = build('youtube','v3', developerKey=api_key)

#Fetch channel data using api key
def fetch_channel_data(newchannel_id):
    try:
         mydb = mysql.connector.connect(
                host=HOST_NAME,
                user=USER_NAME,
                password=PASS,
                database ="youtube_data"
                )
         cursor = mydb.cursor()
         cursor.execute('''CREATE TABLE IF NOT EXISTS Channels(
             channel_id VARCHAR(255) PRIMARY KEY,
             channel_name VARCHAR(255),
             channel_des TEXT,
             channel_playid VARCHAR(255),
             channel_viewcount INT,
             channel_subcount INT)''')
         
         cursor.execute("SELECT * FROM Channels WHERE channel_id = %s",(newchannel_id,))
         exisiting_channel = cursor.fetchone()
         
         if exisiting_channel:
             cursor.close()
             mydb.close()
             st.error("Channel ID Already Exists in The Database")
             return pd.DataFrame()
         
         request = youtube.channels().list(
             part = "snippet,contentDetails,statistics",
             id=newchannel_id
         )
         response = request.execute()
         if 'items' in response and len(response["items"])> 0:
                    #Analyzing the response and extracting the required data
             data ={
                 "channel_id": response["items"][0]["id"],
                 "channel_name": response["items"][0]["snippet"]["title"],
                 "channel_playid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                 "channel_des": response["items"][0]["snippet"]["description"],
                 "channel_views": response["items"][0]["statistics"]["viewCount"],
                 "channel_subcount":response["items"][0]["statistics"]["subscriberCount"]
             }

            #Inserting fetched data into MYSQL database
             cursor.execute(""" INSERT INTO Channels VALUES (%s, %s, %s,%s,%s,%s)""",(data["channel_id"],data["channel_name"],data["channel_des"],data["channel_playid"],data["channel_views"],data["channel_subcount"]))
             mydb.commit()
             cursor.close()
             mydb.close()
             st.success("New Channel Infromation stored Successfully!!")
             return pd.DataFrame(data, index=[0])
         else:
             cursor.close()
             mydb.close()
             st.error("No items found in the response.")
             return pd.DataFrame()
         
    except HttpError as e:
        st.error(f"HTTP ERROR: {e}")
        return pd.DataFrame()
    except KeyError as e:
        st.error(f"KeyError: {e}.Please make sure the channel ID is Correct")
        return pd.DataFrame()

#Fetch video id using channel id
def playlist_videos_id(channel_ids):
    all_video_ids = []
    for newchannel_id in channel_ids:
        videos_ids = [] 
        try:
            # Get the uploads playlist ID from the channel
            response = youtube.channels().list(
                part="contentDetails", id=newchannel_id).execute()
            
            if 'items' in response and len(response["items"]) > 0:
                playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                nextPageToken = None

                while True:
                    # Get the video IDs from the uploads playlist
                    res2 = youtube.playlistItems().list(
                        part="snippet",
                        playlistId=playlist_id, 
                        maxResults=50,
                        pageToken=nextPageToken
                    ).execute()
                    
                    for item in res2.get("items", []):
                        video_id = item["snippet"]["resourceId"]["videoId"]
                        videos_ids.append(video_id)

                    # Check if there is another page of results
                    nextPageToken = res2.get("nextPageToken")
                    if nextPageToken is None:
                        break

            else:
                print(f"No channels found for ID: {newchannel_id}")

        except HttpError as e:
            print(f"HTTP Error: {e}")
        except KeyError as e:
            print(f"KeyError: {e}")
            
        # Append collected video IDs to all_video_ids
        all_video_ids.extend(videos_ids)
        print(f"Video IDs for channel {newchannel_id}: {videos_ids}")       
    return all_video_ids 

#Fetch video data using video IDs
def fetch_video_data(all_videos_ids):
    video_info =[]
    for each in all_videos_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=each
        )
        response = request.execute()
        for i in response["items"]:
            given ={
                       "Video_Id":i["id"] ,
                       "Video_title":i["snippet"]["title"],
                       "Video_Description":i["snippet"]["description"],
                       "channel_id":i['snippet']['channelId'],
                       "Video_Tags": ','.join(i["snippet"].get("tags", [])), 
                       "Video_pubdate":parser.isoparse(i["snippet"]["publishedAt"]),
                       "Video_viewcount":i["statistics"]["viewCount"],
                       "Video_likecount":i["statistics"].get('likeCount',0) ,
                       "Video_favoritecount":i["statistics"]["favoriteCount"],
                       "Video_commentcount":i["statistics"].get("commentCount",0),
                       "Video_duration":iso8601_duration_to_seconds(i["contentDetails"]["duration"]),
                       "Video_thumbnails":i["snippet"]["thumbnails"]['default']['url'],
                       "Video_caption":i["contentDetails"]["caption"]
            }


            video_info.append(given)
    #Inserting fetched data into MYSQL database        
    mydb = mysql.connector.connect(
                host=HOST_NAME,
                user=USER_NAME,
                password=PASS,
                database ="youtube_data"
                )
    cursor = mydb.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Videos (
                      video_id VARCHAR(255) PRIMARY KEY,
                      video_title VARCHAR(255), 
                      video_des TEXT,
                      video_tags VARCHAR(1000),
                      video_pubdate DATETIME,
                      video_viewcount INT,
                      video_likecount INT,
                      video_favcount INT,
                      video_commentcount INT,
                      duration INT,
                      thumbnail VARCHAR(255),
                      captions  VARCHAR(255),
                      channel_id VARCHAR(255),
                      FOREIGN KEY (channel_id) REFERENCES Channels(channel_id)  ON DELETE CASCADE )''')
    
    for video in video_info:
        cursor.execute('''INSERT INTO Videos VALUES(%s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s)''',(video["Video_Id"], 
                       video["Video_title"],video["Video_Description"],video["Video_Tags"],
                       video["Video_pubdate"],video["Video_viewcount"],video["Video_likecount"],video["Video_favoritecount"],
                       video["Video_commentcount"],video["Video_duration"],video["Video_thumbnails"],video["Video_caption"],video["channel_id"]))
        
    mydb.commit()
    mydb.close()

    return pd.DataFrame(video_info)

#Fetch comments from video IDs
def Fetch_comment_data(newchannel_id):
    comment_data =[]
    allvideo_ids = playlist_videos_id([newchannel_id])
    
    for video in allvideo_ids:
        
        try:
            req = youtube.commentThreads().list(
                part="snippet",
                videoId=video,
                maxResults=500,
                )
            res = req.execute()
            for i in res["items"]:
                output ={
                           "comment_id":i["snippet"]["topLevelComment"]["id"],
                            "Comment_Text":i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            "Comment_Authorname":i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            "published_date":parser.isoparse(i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]),
                            "video_id":i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            'channel_id':i['snippet']['channelId']}
                
                comment_data.append(output)
            nextpagetoken= res.get('nextPageToken')
            

        except HttpError as e:
            print(f"An HTTP error Occurred :{e}")
            break

    if not comment_data:
        st.text("No comment data found.")
        return pd.DataFrame()
    #Inserting the fetched data into MYSQL database
    mydb = mysql.connector.connect(
                host=HOST_NAME,
                user=USER_NAME,
                password=PASS,
                database ="youtube_data"
                )
    cursor = mydb.cursor()

    cursor.execute('''CREATE TABLE  IF NOT EXISTS Comments (comment_id VARCHAR(255) PRIMARY KEY ,
                        comment_text TEXT,
                        authorname VARCHAR(255),comment_pubdate DATETIME,video_id VARCHAR(255),channel_id VARCHAR(255),
                        FOREIGN KEY (video_id) REFERENCES Videos(video_id),
                        FOREIGN KEY (channel_id) REFERENCES Channels(channel_id))''')

    for comment in comment_data:
            
        cursor.execute('''INSERT INTO Comments  (comment_id, comment_text, authorname, comment_pubdate, video_id, channel_id)
                           VALUES(%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
                              comment_text = VALUES(comment_text),
                                authorname = VALUES(authorname),
                                comment_pubdate = VALUES(comment_pubdate),
                                video_id = VALUES(video_id),
                                channel_id = VALUES(channel_id); ''',
                           (comment["comment_id"],comment["Comment_Text"],comment["Comment_Authorname"],
                             comment["published_date"],comment["video_id"],comment["channel_id"]))
    mydb.commit()
    mydb.close()
                
        
    return pd.DataFrame(comment_data)
    
    
#Convert the duration from hours to seconds   
def iso8601_duration_to_seconds(duration):
    # Match ISO 8601 duration format (PT#H#M#S)
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        print(f"Invalid ISO 8601 duration format: {duration}")
        return None

    # Extract hours, minutes, and seconds
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    # Calculate total seconds
    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

#Fetch data from MYSQL database
def fetch_data(query):
    mydb =  mysql.connector.connect(
                host=HOST_NAME,
                user=USER_NAME,
                password=PASS,
                database ="youtube_data"
                )
    df = pd.read_sql(query, mydb)
    mydb.close()
    return df 

#Execute the predefined queries
def execute_query(question):
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
		         """SELECT video_title,channel_name 
                 FROM Videos AS v
                 JOIN channels ON channels.channel_id=v.channel_id;""",
        "Which channels have the most number of videos, and how many videos do they have?": 
		         """SELECT channel_name, COUNT(video_id) AS video_count
				 FROM Videos AS v
                 JOIN Channels ON channels.channel_id=v.channel_id
                 GROUP BY channel_name
                 ORDER BY video_count DESC;""",
        "What are the top 10 most viewed videos and their respective channels?": 
		         """SELECT video_title,channel_name 
                 FROM Videos AS v
                 JOIN Channels AS ch ON ch.channel_id =v.channel_id 
                 ORDER BY video_viewcount DESC 
                 LIMIT 10;""",
        "How many comments were made on each video, and what are their corresponding video names?": 
		         """SELECT video_title, COUNT(*) AS comment_counts
                 FROM Videos 
                 JOIN Comments on Videos.video_id=Comments.video_id
                 GROUP BY video_title;""",
        "Which videos have the highest number of likes, and what are their corresponding channel names?": 
		         """SELECT video_title,channel_name
                 FROM Videos 
                 JOIN Channels ON Channels.channel_id=Videos.channel_id
                 ORDER BY video_likecount DESC
                 LIMIT 1;""",
        "What is the total number of likes for each video, and what are their corresponding video names?":	          
                """SELECT Videos.video_title, SUM(Videos.Video_likecount) AS total_likes
                  FROM Videos
                  GROUP BY videos.video_title;""",
        "What is the total number of views for each channel, and what are their corresponding channel names?": 
		          """SELECT channel_name, SUM(video_viewcount) AS Total_views
                  FROM Videos
                  JOIN Channels ON Channels.channel_id=Videos.channel_id
                  GROUP BY channel_name;""",
        "What are the names of all the channels that have published videos in the year 2022?": 
		          """SELECT DISTINCT Channels.channel_name
                  FROM Channels
                  JOIN Videos ON Channels.channel_id = Videos.channel_id
                  WHERE YEAR(Videos.Video_pubdate) = 2022;""",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?": 
		          """ SELECT channel_name,AVG(duration) AS Average_duration
                  FROM Videos
                  JOIN Channels ON Videos.channel_id = Channels.channel_id
                  GROUP BY channel_name;""",
        "Which videos have the highest number of comments, and what are their corresponding channel names?": 
		          """ SELECT video_title,channel_name
                  FROM Videos
                  JOIN Channels ON Videos.channel_id = Channels.channel_id
                  ORDER BY video_commentcount DESC
                  LIMIT 1;""" 
    }

    query=query_mapping.get(question)
    if query:
        return fetch_data(query)
    else:
        return pd.DataFrame()
    
      

#Streamlit coding part to showcase the output in streamlit
def main():
    st.title("Youtube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")
    
    Options = st.sidebar.radio("Options", ("Channels","Videos","Comments","Queries","Enter YouTube Channel ID"))
    
    if  Options == "Channels":
        st.header("Channels")
        channels_df = fetch_data("SELECT * FROM Channels;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif Options == "Videos":
        st.header("Videos")
        videos_df = fetch_data("SELECT * FROM Videos;")
        videos_df.index += 1
        st.dataframe(videos_df)

    elif Options == "Comments":
        st.header("Comments")
        comments_df = fetch_data("SELECT * FROM Comments;")
        comments_df.index += 1
        st.dataframe(comments_df)

    elif Options == "Queries":
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
            "Which videos have the highest number of comments, and what are their corresponding channel names?"])
                                      
        if query_question:
            query_result_df =execute_query(query_question)
            query_result_df.index += 1
            st.dataframe(query_result_df) 
    elif Options == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")
        if st.button("Fetch Channel Data"):
            channel_df = fetch_channel_data(channel_id)
            channel_df.index +=1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch Video Data"):
            all_video_ids = playlist_videos_id([channel_id])
            video_df = fetch_video_data(all_video_ids)
            video_df.index +=1
            st.subheader("Video Data")
            st.write(video_df)

        if st.button("Fetch Comment Data"):
            comment_df = Fetch_comment_data([channel_id])
            comment_df.index +=1
            st.subheader("Comment Data")
            st.write(comment_df)

if __name__ == "__main__":
    main()