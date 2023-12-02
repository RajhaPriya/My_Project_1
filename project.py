import googleapiclient.discovery
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import pprint
import json
import streamlit as st
import pymysql

api_key = "AIzaSyAvtvUaoQUZ5szcDR6Bpn0frAM6Bww5dVo"
youtube = build('youtube','v3', developerKey=api_key)

#channel --> fetch
def get_channel(youtube, channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()
    
    for i in response['items']:
        data = dict(Channel_name = response['items'][0]['snippet']['title'],
                    Channel_id = response['items'][0]['id'],
                    Description = response['items'][0]['snippet']['description'],
                    Subscribers = response['items'][0]['statistics']['subscriberCount'],
                    Views = response['items'][0]['statistics']['viewCount'],
                    Videos = response['items'][0]['statistics']['videoCount'],
                    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return data

#video_id --> fetch
def get_videos_ids(youtube, playlist_id):

        video_ids = []

        request = youtube.playlistItems().list(part="contentDetails",playlistId=playlist_id,maxResults=50)
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages:

            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=20,
                    pageToken=next_page_token)

                response = request.execute()
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')

        return video_ids

#comments --> fetch
def comment_details(video_ids):
    comments = []
    for i in video_ids:
        try:
            request = youtube.commentThreads().list(part='snippet,replies',videoId=i,maxResults=50)
            response = request.execute()
            if len(response['items'])>0:
                for j in range(len(response['items'])):
                    comments.append({
                        'comment_id': response['items'][j]['snippet']['topLevelComment']['id'],
                        'video_id': i,
                        'Comment_Author': response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Text': response['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'Comment_PublishedAt':response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''),
                        'Comment_Likes': int(response['items'][j]['snippet']['topLevelComment']['snippet']['likeCount'])
                    })
                    
        except:
            pass
    return comments
   
#videos --> fetch
youtube = build('youtube','v3', developerKey=api_key)

def get_video_data(youtube, video_ids):
    video_info = []

    for i in video_ids:
        try:
            request = youtube.videos().list(part='snippet,statistics',id=i)
            response = request.execute()

            data = dict(Video_title = response['items'][0]['snippet']['title'],
                        Video_description = response['items'][0]['snippet']['description'],
                        Video_id = i,
                        Channel_id = response['items'][0]['snippet']['channelId'],
                        Video_published = response['items'][0]['snippet']['publishedAt'],
                        Video_views = response['items'][0]['statistics']['viewCount'],
                        Video_likes = response['items'][0]['statistics']['likeCount'],
                        Video_comments = response['items'][0]['statistics']['commentCount']
            )

            video_info.append(data)

        except:
            pass

    return video_info

#mysql connection
py = pymysql.connect(
        host="localhost",
        user="root",
        password="123",
        database="project")
mycursor = py.cursor()
mycursor.execute("use project")

#creating tables in sql
def create_table():
    mycursor.execute( '''CREATE TABLE IF NOT EXISTS channel (Channel_name VARCHAR(200),Channel_id VARCHAR(200),Description TEXT,Subscribers VARCHAR(200),Views VARCHAR(200),Videos VARCHAR(200),playlist_id VARCHAR(200));''')

    mycursor.execute( ''' CREATE TABLE IF NOT EXISTS video (
    Video_title TEXT,
    Video_description TEXT,
    Video_id VARCHAR(200),
    Channel_id VARCHAR(200),
    Video_published VARCHAR(200),
    Video_views VARCHAR(200),
    Video_likes VARCHAR(200),
    Video_comments VARCHAR(200));''')

    mycursor.execute('''CREATE TABLE IF NOT EXISTS comment (comment_id VARCHAR(200),
    video_id VARCHAR(200),
    Comment_Author VARCHAR(200),
    Comment_Text TEXT,
    Comment_PublishedAt VARCHAR(200),
    Comment_Likes VARCHAR(200));''')

def insert_sql(channel_id):
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    db = myclient["mydatabase"]
    col = db["youtube"]
    f = col.find_one({'channel.Channel_id': channel_id}, {'_id': 0})
    create_table()
    existing_channel_query = 'SELECT Channel_id FROM channel WHERE Channel_id = %s'
    mycursor.execute(existing_channel_query, (channel_id,))
    existing_channel = mycursor.fetchone()

    if existing_channel:
        print('Channel already exists:', channel_id)
        return

    sql = ''' INSERT INTO channel (
        Channel_name,
        Channel_id,
        Description,
        Subscribers,
        Views,
        Videos,
        playlist_id ) VALUES (%s, %s, %s, %s, %s, %s, %s) '''
    val = tuple(f['channel'].values())
    mycursor.execute(sql, val)
    py.commit()

    val_1 = []
    for item in f['video']:
        v = tuple(item.values())
        val_1.append(v)

    sql_1 = '''INSERT INTO video (Video_title, Video_description, Video_id, Channel_id, Video_published, Video_views,
            Video_likes, Video_comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

    mycursor.executemany(sql_1, val_1)
    py.commit()

    val_2 = []
    for i in f['comment']:
        c = tuple(i.values())
        val_2.append(c)

    sql_2 = '''
        INSERT INTO comment (
            comment_id, video_id, Comment_Author, Comment_Text, Comment_PublishedAt, Comment_Likes
        ) VALUES (%s, %s, %s, %s, %s, %s)'''

    mycursor.executemany(sql_2, val_2)
    py.commit()

#main_table -- > sql
def tables():
    insert_sql(channel_id)
    return "Table successfully inserted"

#main function 
def main(channel_id):
    c_data = get_channel(youtube, channel_id)
    video_ids = get_videos_ids(youtube, c_data['playlist_id'])  #creating connection of playlist - channel
    v_info = get_video_data(youtube, video_ids)
    com_data = comment_details(video_ids)
    d = {'channel': c_data,'video': v_info, 'comment': com_data}
    return d

#insert to mongodb
def insert_mongodb(data, channel_id):
    c_id = []
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    db = myclient["mydatabase"]
    col = db["youtube"] 
    for item in col.find({},{"_id":0, "channel":1}):
        c_id.append(item['channel']['Channel_id'])
    if channel_id in c_id:
        st.success("YouTube channel details for the given channel id exist")
    else:
        col.insert_one(data)
        st.success(f"Inserted YouTube channel details for {channel_id}")

#df table for view 
def show_channels_table():
    ch_list = []
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    db = myclient["mydatabase"] 
    col = db["youtube"]
    for i in col.find({},{"_id":0,"channel":1}):
        print(i)
        ch_list.append(i["channel"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_videos_table():
    vi_list = []
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    db = myclient["mydatabase"] 
    col = db["youtube"]
    for item in col.find({},{"_id":0,"video":1}):
        for i in range(len(item["video"])):
            vi_list.append(item["video"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    myclient = pymongo.MongoClient('mongodb://localhost:27017')
    db = myclient["mydatabase"] 
    col = db["youtube"]
    for item in col.find({},{"_id":0,"comment":1}):
         for i in range(len(item["comment"])):
            com_list.append(item["comment"][i])
    comments_table = st.dataframe(com_list)
    return comments_table


#streamlit setup
st.set_page_config(layout="wide")
st.subheader("Platform to extract and retrieve your youtube datas in a click")

with st.sidebar:
    st.title(':green[Youtube Data Harvesting]')
    st.image("girl.jpg")
    st.header('Roadmap')
    st.caption('Youtube_API')
    st.caption('MongoDB')
    st.caption('SQL')
    st.caption('Streamlit')
channel_id = st.text_input("Enter the channel id here")

data = {}
if st.button("Scrap and push"):
    data = main(channel_id)
    insert_mongodb(data, channel_id)
    st.write(data)

if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)   

show_table = st.radio("SELECT THE TABLE FOR VIEW",(":violet[channels]",":violet[videos]",":violet[comments]"))

if show_table == ":violet[channels]":
    show_channels_table()
elif show_table ==":violet[videos]":
    show_videos_table()
elif show_table == ":violet[comments]":
    show_comments_table() 


st.header(':green[Channel - Related explore options]')

#sql_connection 
search_connection = pymysql.connect(
        host="localhost",
        user="root",
        password="123",
        database="project")
mycursor = search_connection.cursor()

question = st.selectbox ("Select your question",
                       ['1. What are the names of all the videos and their corresponding channels?',
                      '2. Which channels have the most number of videos, and how many videos do they have?',
                      '3. What are the top 10 most viewed videos and their respective channels?',
                      '4. How many comments were made on each video, and what are their corresponding video names?'
                      '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                      '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                      '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                      '8. What are the names of all the channels that have published videos in the year 2022?',
                      '9. Which videos have the highest number of comments, and what are their corresponding channel names?'],  index = None )
search = question
st.write("You selected:", search)

if search == '1. What are the names of all the videos and their corresponding channels?':
    mycursor.execute("SELECT `channel`.`Channel_name`, `video`.`Video_title` FROM `channel` INNER JOIN `video` ON `channel`.`Channel_id` = `video`.`Channel_id`;")
    result_1 = mycursor.fetchall()
    df1 = pd.DataFrame(result_1, columns=['Channel_name', 'Video_title'])
    st.dataframe(df1)
    
elif search =='2. Which channels have the most number of videos, and how many videos do they have?':
    mycursor.execute("SELECT channel.Channel_id, channel.Channel_name, COUNT(video.Video_id) AS Video_count FROM channel INNER JOIN video ON channel.Channel_id = video.Channel_id GROUP BY channel.Channel_id, channel.Channel_name ORDER BY Video_count DESC;")
    result_2 = mycursor.fetchall()
    df2 = pd.DataFrame(result_2, columns=['Channel_id', 'Channel_name', 'Video_count'])
    st.dataframe(df2)

elif search =='3. What are the top 10 most viewed videos and their respective channels?':
    mycursor.execute("Select video.Video_id, video.Video_title, video.Video_views, channel.Channel_name from video inner join channel on video.Channel_id = channel.Channel_id order by Video_views asc limit 10;")
    result_3 = mycursor.fetchall()
    df3 = pd.DataFrame(result_3, columns=['Video_id', 'Video_title', 'Video_views', 'Channel_name'])
    st.dataframe(df3)

elif search =='4. How many comments were made on each video, and what are their corresponding video names?':
    mycursor.execute("SELECT count(comment.comment_id) as Comment_count, video.Video_title from video left join comment on video.Video_id = comment.video_id group by video.Video_title;")
    result_4 = mycursor.fetchall()
    df4 = pd.DataFrame(result_4, columns = ['Comment_count', 'Video_title'])
    st.dataframe(df4)

elif search =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    mycursor.execute("SELECT video.Video_likes, video.Video_title, video.Video_id, channel.channel_name from video inner join channel on video.Channel_id = channel.Channel_id order by Video_likes desc limit 5;")
    result_5 = mycursor.fetchall()
    df5 = pd.DataFrame(result_5, columns = ['Video_likes', 'Video_title', 'Video_id', 'channel_name'])
    st.dataframe(df5)

elif search =='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    mycursor.execute("Select video.Video_id, video.Video_title, SUM(video.Video_likes) as Total_likes from video group by video.Video_id, video.Video_title;")
    result_6 = mycursor.fetchall()
    df6 = pd.DataFrame(result_6, columns = ['Video_id', 'Video_title', 'Total_likes'])
    st.dataframe(df6)

elif search =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    mycursor.execute("select channel.Channel_name, channel.Channel_id, SUM(video.Video_views) AS Total_views from channel left join video on channel.Channel_id = video.Channel_id group by channel.Channel_name, channel.Channel_id;")
    result_7 = mycursor.fetchall()
    df7 = pd.DataFrame(result_7, columns = ['Channel_name', 'Channel_id', 'Total_views'])
    st.dataframe(df7)

elif search =='8. What are the names of all the channels that have published videos in the year 2022?':
    mycursor.execute("SELECT DISTINCT channel.Channel_name FROM channel INNER JOIN video ON channel.Channel_id = video.Channel_id WHERE YEAR(STR_TO_DATE(video.video_published, '%Y-%m-%dT%H:%i:%sZ')) = 2022;")
    result_8 = mycursor.fetchall()
    df8 = pd.DataFrame(result_8, columns = ['Channel_name'])
    st.dataframe(df8)

elif search =='9. Which videos have the highest number of comments, and what are their corresponding channel names?':
    mycursor.execute("SELECT video.Video_id, video.Video_title, channel.Channel_name, COUNT(comment_id) AS Comment_count FROM video INNER JOIN channel ON video.Channel_id = channel.Channel_id LEFT JOIN comment ON video.Video_id = comment.video_id GROUP BY video.Video_id, video.Video_title, channel.Channel_name ORDER BY Comment_count DESC LIMIT 10;")
    result_9 = mycursor.fetchall()
    df9 = pd.DataFrame(result_9, columns= ['Video_id', 'Video_title', 'Channel_name', 'Comment_count'])
    st.dataframe(df9)

search_connection.close()