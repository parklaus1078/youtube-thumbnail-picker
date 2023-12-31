# youtube-thumbnail-picker
## Objective
This project returns the thumbnails of 5 videos with the greatest number of views over hours uploaded on a channel
## Keywords
- Web Crawling
- Airflow
- Python
- ETL
- MongoDB
## Process
1. It takes a link of a Youtube Channel as an input
2. Crawls the thumbnails, the number of views, and the upload dates of the videos
   2-1. If one of the data are not extracted, task fails.
   2-2. if a task fails, send an email.
4. Divides the number of view by the hours since the video was uploaded(I'd call this number as view per hour)
5. Returns the thumbnails of 5 videos that has highest view per hour
