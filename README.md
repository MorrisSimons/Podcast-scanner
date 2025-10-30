
# Podcast DB

### GOAL

Find out **who’s talking about a company or person** in any Swedish podcast. Also just cool to have this kind of data and database.


### Techstack

1. Supabase free tier to spin up a simple database to store the early data.

2. When we get more data and need to store files the plan is to use scaleway S3 storage, also here free tier as far as possible.

3. When we have the MP3 we need to transcribe all the podcasts here im going to use a couple of H200 GPUs, we have a small GPU cluster of 8 of them for 500,000 euro at work; Thanks boss ly.

4. in the gpu cluster we are going to throw in a whisper model to transcibe the data or mabye a model that is good a swedish.

5. last step build my own search engine or pinecone; TBD

6. then a nice UI. 

---

### PLAN

Scrape all podcasts in Sweden and build a searchable database.

---

### WHAT WE DO

#### Step 1

Get all podcasts in Sweden.

#### Step 2

Get each podcast’s profile and RSS feed URL.

#### Step 3

Fetch each RSS file and save it (plus metadata).

#### Step 4

Process RSS data for search and analysis.

---

### BACKGROUND

I wanted a way to find where all Swedish podcasts are listed and found this endpoint:
`https://api.mediafacts.se/api/podcast/v1/podcasts`

That request returns every podcast ID and name in Sweden.
With that ID, we can get full details:

```bash
curl --location 'https://api.mediafacts.se/api/podcast/v1/podcasts/details?id=857538db-0c16-4f7d-b053-08dc85405cb3'
```

Example response:

```json
{
  "rssFeedUrl": "https://rss.podplaystudio.com/1477.xml",
  "podcastName": "Fallen jag aldrig glömmer",
  "supplierName": "Podplay",
  "networkName": "Bauer Media",
  "genre": "Thriller/Crime"
}
```

Main target: `rssFeedUrl`
Keep the rest (supplier, network, genre) for future analysis.

I’m using Supabase to store all data for fast iteration.

---

### STATUS

**Step 1 – Get podcast profiles**
Success rate: **99.04%**

```
status_code Distribution:
================================================================================
200                   2377 (99.04%)
404                     19 ( 0.79%)
400                      1 ( 0.04%)
410                      1 ( 0.04%)
500                      1 ( 0.04%)
503                      1 ( 0.04%)


Total unique status_code: 6
Null/Missing status_code: 0
```

Conslusion; Minor dataloss we dont cate to fix it now

```
RSS_request_status_code Distribution:
================================================================================
200                   2315 (96.46%)
404                     77 ( 3.21%)
400                      2 ( 0.08%)
410                      2 ( 0.08%)
500                      2 ( 0.08%)
503                      1 ( 0.04%)
0                        1 ( 0.04%)


Total unique RSS_request_status_code: 7
Null/Missing RSS_request_status_code: 0
```


Conclusion; Here the data loss is a bit more significant; But still enough. 

---

### RSS SOURCE DISTRIBUTION

Most feeds come from a few platforms:

```
Base URL Distribution:
================================================================================
https://feeds.acast.com                             1136 (47.33%)
https://feed.pod.space                               521 (21.71%)
https://api.sr.se                                    272 (11.33%)
https://rss.podplaystudio.com                        150 ( 6.25%)
https://rss.acast.com                                109 ( 4.54%)
https://podcast.stream.schibsted.media                94 ( 3.92%)
https://access.acast.com                              70 ( 2.92%)
https://feed.khz.se                                   20 ( 0.83%)
https://cdn.radioplay.se                              12 ( 0.50%)
http://www.ilikeradio.se                               8 ( 0.33%)
https://www.ilikeradio.se                              8 ( 0.33%)


Total unique base URLs: 11
Null/Missing RSS URLs: 0
```

Conslusion; This means that the structure is likley the same on all these platforms. Looking at the data we can see that most of the follow the same structure with Itunes tags with same names and title and links etc are the same. That consistency makes the ETL pipeline easy to build and maintain.

---

### RSS STRUCTURE

Most of them follow the same iTunes XML layout:

```xml
<item>
  <title>705. Lex Birgitta Ed</title>
  <pubDate>Thu, 23 Oct 2025 22:58:22 +0000</pubDate>
  <guid isPermaLink="false">7258a43677f100f9d45ef43f19395a99</guid>
  <link>https://pod.space/alexosigge/705-lex-birgitta-ed</link>
  <itunes:image href="https://assets.pod.space/system/shows/images/397/fef/88-/large/Alex_och_Sigge.jpg"/>
  <description><![CDATA[]]></description>
  <enclosure url="https://media.pod.space/alexosigge/aos705.mp3" type="audio/mpeg"/>
  <itunes:duration>00:59:21</itunes:duration>
</item>
```


### we manage to scrape 

