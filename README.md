# YouTube Data Harvesting and Warehousing using SQL and Streamlit

## Problem Statement

The task is to build a Streamlit app that allows users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data such as channel information, video details, and user engagement metrics like views, likes, and comments. The app should store this data in a SQL database and support querying and retrieving data from up to 10 different channels (as a user-defined constraint). Advanced querying options should be provided, including the ability to join tables for comprehensive analysis of channel and video data.

## Technology Stack Used

1. Python
2. MySQL 
3. Google API Client Library for Python (YouTube API)
4. Streamlit (for building the web interface)

## Approach

1. **Streamlit Application**: 
   - Create a user-friendly interface using the Streamlit Python library. The app will allow users to enter YouTube channel IDs, view the relevant channel details (e.g., channel name, subscriber count), and select up to 10 channels for data collection.

2. **YouTube API Integration**:
   - Establish a connection to the YouTube Data API (v3) via the Google API client library for Python. This allows the retrieval of data related to channels, videos, and engagement metrics (likes, views, comments).

3. **Data Storage**:
   - Collect and store data in a structured SQL database (e.g., MySQL). The data will be stored across multiple tables: one for channels, one for videos, and one for comments. Each table will be related via primary and foreign keys (e.g., a foreign key linking videos to channels).

4. **SQL Queries and Data Retrieval**:
   - Use SQL queries to join tables in the database and retrieve specific data as requested by the user. For example, users may want to view the total number of views across all videos from a specific channel. The database schema will support complex queries by ensuring proper relationships between the tables using primary and foreign keys.

5. **Data Display and Analysis**:
   - Display the retrieved data within the Streamlit application. The data will be presented in a user-friendly format, allowing users to analyze the performance of different channels and videos. It help users better understand the data (e.g., growth trends over time, video performance comparisons).
