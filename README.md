# Web server logs analysis

An apache spark ETL pipeline to ingest HTTP web server logs and analyse using matplotlib and sparkSQL.

## Data description
- Source: https://ita.ee.lbl.gov/html/contrib/Calgary-HTTP.html
- This trace contains approximately one year's worth of all HTTP requests to the University of Calgary's Department of Computer Science WWW server located at Calgary, Alberta, Canada. 
- The logs were collected from October 24, 1994 through October 11, 1995, a total of 353 days. There were 726,739 requests. Timestamps have 1 second resolution. 
- The logs are an ASCII file with one line per request, with the following columns:
    - **host** making the request. Hosts are identified as either local or remote where local is a host from the University of Calgary, and remote is a host from outside of the University of Calgary domain.
    - **timestamp** in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0700 between 30/Oct/1994:01:30:57 and 02/Apr/1995:03:03:26. For all other requests, the timezone is -0600.
    - **filename** of the requested item. Paths have been removed. Modified filenames consist of two parts: num.type , where num is a unique integer identifier, and type is the extension of the requested file.
    - **HTTP reply code**.
    - **bytes** in the reply.

## Pipeline

- Read logs from text file
- Use regex to extract groups. Groups correspond to columns
- Update schema of dataframe
- Add new columns using transformations
- Partition and store in parquet format

## Analysis

- Most requested file types
- Reply size stats
- Avg reply size over time
- HTTP methods distribution
- Avg reply size by HTTP method
- Request count by weekday