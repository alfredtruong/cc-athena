# tutorials
#   - https://frankcorso.dev/querying-athena-python.html
#   - https://medium.com/codex/connecting-to-aws-athena-databases-using-python-4a9194427638
# approaches
# (1) pyathena + sqlachemy  # slow but easy
# (2) boto3                 # fast but harder to setup
#   - https://realpython.com/python-boto3-aws-s3/#:~:text=Boto3%20is%20the%20name%20of,resources%20from%20your%20Python%20scripts

# https://groups.google.com/g/common-crawl/c/t_H0yeL26eY/m/INT0uh7R5-IJ
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/corefunctions.ipynb
# https://github.com/brevityinmotion/straylight/blob/main/notebooks/tools-commoncrawl.ipynb

#%%
import pandas as pd
from athena import AthenaQuery

pd.set_option('display.max_colwidth', None)

#%%
########################################
# sqlalchemy
########################################
INDEX = '2018-34'
aq = AthenaQuery()

#%%
########################################
# [query] post-2018-34
########################################
query = fr'''
SELECT *
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-{INDEX}'
  AND subset = 'warc'                               -- filter by subset
  AND url_host_tld = 'com'                          -- domain must end with .com
  AND fetch_status = 200                            -- must be successful request
  AND content_languages LIKE '%zho%'                -- must contain chinese
  AND content_mime_type = 'text/html'               -- only care about htmls
--  AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
--LIMIT 10;
'''
res=aq.do_query(query.format(index=INDEX))
res

#%%
########################################
# [query] post-2018-34
########################################
query = fr'''
SELECT *
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-{INDEX}'
  AND subset = 'warc'                               -- filter by subset
  AND url_host_tld = 'com'                          -- domain must end with .com
  AND fetch_status = 200                            -- must be successful request
  AND content_languages LIKE '%zho%'                -- must contain chinese
  AND content_mime_type = 'text/html'               -- only care about htmls
  AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
LIMIT 10;
'''
res=aq.do_query(query.format(index=INDEX))
res

#%%
# [query] before 2018
query = fr'''
SELECT *
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-{INDEX}'
  AND subset = 'warc'                               -- filter by subset
  AND url_host_tld = 'com'                          -- domain must end with .com
  AND fetch_status = 200                            -- must be successful request
--  AND content_languages LIKE '%zho%'                -- must contain chinese
  AND content_mime_type = 'text/html'               -- only care about htmls
  AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
LIMIT 10;
'''
res=aq.do_query(query.format(index=INDEX))
res

########################################
# [query] understand content_mime_type and content_mime_detected
########################################
# https://blog.isosceles.com/how-to-build-a-corpus-for-fuzzing/
# content_mime_type       # content type (sometimes called MIME type) is an identifier that's commonly sent in HTTP responses to indicate the file format of the response
# content_mime_detected   # Common Crawl goes one step further, it attempts to confirm the real file format by using the "Content Detection" feature of Apache Tika. This is what Common Crawl's index calls the "content_mime_detected"

########################################
# [query] content_mime_detected
########################################
query = r'''
SELECT COUNT(*) as n_pages,
       content_mime_detected
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-26'
  AND subset = 'warc'                           -- filter by subset
  AND url_host_tld = 'com'                      -- domain must end with .com
  AND fetch_status = 200                        -- must be successful request
GROUP BY content_mime_detected
ORDER BY n_pages DESC;'''
res=aq.do_query(query)
res
'''
n_pages	content_mime_detected
0	1126440057	text/html
1	107869242	application/xhtml+xml
2	3857167	application/pdf
3	2425156	application/atom+xml
4	905118	application/rss+xml
5	586408	text/plain
6	460283	application/xml
7	227503	text/calendar
8	205518	application/json
9	67772	text/prs.lines.tag
10	61529	application/octet-stream
11	33243	text/x-vcard
12	33013	text/x-php
13	27645	application/vnd.google-earth.kml+xml
'''

#%%
########################################
# [query] content_mime_type
########################################
query = r'''
SELECT COUNT(*) as n_pages,
       content_mime_type
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-26'
  AND subset = 'warc'                           -- filter by subset
  AND url_host_tld = 'com'                      -- domain must end with .com
  AND fetch_status = 200                        -- must be successful request
GROUP BY content_mime_type
ORDER BY n_pages DESC;
'''
res=aq.do_query(query)
res

#%%
########################################
# [query] record information
########################################
query = r'''
SELECT 
  url,
  url_host_name,
  url_host_registered_domain,
  content_digest,
  warc_filename,
  warc_record_offset,
  warc_record_length
FROM (
  SELECT
    url,
    url_host_name,
    url_host_registered_domain,
    content_digest,
    warc_filename,
    warc_record_offset,
    warc_record_length,
    ROW_NUMBER() OVER (PARTITION BY content_digest ORDER BY warc_record_length ASC) AS rn
  FROM "ccindex"."ccindex"
  WHERE crawl = 'CC-MAIN-2024-26'
    AND subset = 'warc'
    AND url_host_tld = 'com'
    AND fetch_status = 200
    AND content_languages LIKE '%zho%'
    AND (content_mime_type = 'text/html' OR content_mime_detected = 'text/html')
    AND url_host_name LIKE '%hk%'
  -- AND url_host_name NOT LIKE '%.search.%'
  -- AND url_host_name NOT LIKE '%.promotion.%'
  -- AND url_host_name NOT LIKE '%.promotions.%'
  -- AND url_host_name NOT LIKE '%.tv.%'
  -- AND url_host_name NOT LIKE '%.mobile.%'
  -- AND url_host_name NOT LIKE '%.mail.%'
  -- AND url_host_name NOT LIKE '%.help.%'
  -- AND url NOT LIKE '%error%'
  -- AND warc_record_length > 100000
  -- AND url_host_registered_domain = 'yahoo.com'
) t
WHERE rn = 1
ORDER BY warc_record_length ASC;
'''
res=aq.do_query(query)

#%%
res2 = res.url_host_name.value_counts()
url_host_name_subset = res2[res2 > 5000].index
res3 = res[res.url_host_name.isin(url_host_name_subset)]
res3.to_pickle('data/finalized/2024-26_common_url_host_name.pkl')

#%%
res4 = res3.groupby('url_host_name').apply(
  lambda x:pd.Series(
    {
      'count':len(x),
      'len_min':int(x['warc_record_length'].min()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_med':int(x['warc_record_length'].median()),
      'len_max':int(x['warc_record_length'].max()),
      'example':x['url'].iloc[0],
    }
  )  
).sort_values('count',ascending=False)
res4

#%%
res4[res4['count']>0]['count'].plot()

#%%
res4['count'].sum()

#%%
########################################
# [query] record information
########################################
query = r'''
SELECT
  FIRST(t1.url) AS url,                               -- full url
  FIRST(t1.url_host_name) AS url_host_name,           -- short url
  FIRST(t1.url_host_registered_domain) AS url_host_registered_domain,
  t1.content_digest,                                  -- SHA-1 hashing of content
  FIRST(t1.warc_filename) AS warc_filename,           -- which warc
  FIRST(t1.warc_record_offset) AS warc_record_offset, -- where to start
  FIRST(t1.warc_record_length) AS warc_record_length  -- how far to go
FROM (
  SELECT
    url,                                              -- full url # https://www.example.com/path/index.html
    url_host_name,                                    -- url_host_name # www.example.com
    url_host_registered_domain,                       -- short url # example.com
    content_digest,                                   -- SHA-1 hashing of content
    warc_filename,                                    -- which warc
    warc_record_offset,                               -- where to start
    warc_record_length                                -- how far to go
  FROM "ccindex"."ccindex"
  WHERE crawl = 'CC-MAIN-{index}'                     -- filter by crawl
    AND subset = 'warc'                               -- filter by subset
    AND url_host_tld = 'com'                          -- domain must end with .com
    AND fetch_status = 200                            -- must be successful request
    AND content_languages LIKE '%zho%'                -- must contain chinese
    AND (content_mime_type = 'text/html' OR content_mime_detected = 'text/html') -- only care about htmls
    AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
  GROUP BY
    content_digest
  HAVING COUNT(content_digest) = 1
) t1
GROUP BY
  t1.content_digest
ORDER BY
  FIRST(t1.warc_record_length) ASC;
'''
query = r'''
  SELECT
    url,                                              -- full url # https://www.example.com/path/index.html
    url_surtkey,                                      -- sortable url # com,example)/path/index.html
    url_host_name,                                    -- url_host_name # www.example.com
    url_host_registered_domain,                       -- short url # example.com
    content_digest,                                   -- SHA-1 hashing of content
    warc_filename,                                    -- which warc
    warc_record_offset,                               -- where to start
    warc_record_length                                -- how far to go
  FROM "ccindex"."ccindex"
  WHERE crawl = 'CC-MAIN-{index}'                     -- filter by crawl
    AND subset = 'warc'                               -- filter by subset
    AND url_host_tld = 'com'                          -- domain must end with .com
    AND fetch_status = 200                            -- must be successful request
    AND content_languages LIKE '%zho%'                -- must contain chinese
    AND (content_mime_type = 'text/html' OR content_mime_detected = 'text/html') -- only care about htmls
    AND url_host_name LIKE '%hk%'                     -- url_host_name must contain hk
'''
res=aq.do_query(query.format(index=INDEX))
res


#%%
# [query] domain profiling
query = r'''
SELECT COUNT(*) AS count,
       url_host_registered_domain
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-26'                 -- filter by crawl
  AND subset = 'warc'                           -- filter by subset
  AND url_host_tld = 'com'                      -- domain must end with .com
  AND fetch_status = 200                        -- must be successful request
  AND content_mime_type = 'text/html'           -- only care about htmls
  AND content_languages LIKE '%zho%'            -- must contain chinese
  AND url LIKE '%hk%'                           -- url must contain hk
GROUP BY url_host_registered_domain
HAVING (COUNT(*) >= 100)
ORDER BY count DESC;
'''
res=aq.do_query(query)

#%%
########################################
# [query] load cached results
########################################
# domain count filtering
#_hash = '3526ee9a8f6ab134c5d9f40fd8cfd227' # all urls

# building filter
#_hash = 'd5044f387e1f735523b3b9a2c487670c' # 0, empty, url_host_registered_domain LIKE %hk% too strong
#_hash = 'aa61aee1a98459c610faa011f9917b23' # 1, only yahoo
#_hash = 'c0f8dfe594f0039ad15c33ec41f56cb2' # 2, only yahoo, contains tw in url_host_name
#_hash = 'a86b4fa51dfefbc733f6a51192fb4ecc' # 3, only yahoo, removed 'tw', need to remove ".search.", ".tv." and ".promotions."
#_hash = '2ea23635c81430d6d041039a4e19cf80' # 4, only yahoo, remove but didnt work
#_hash = 'ecad2809e16409a70d606d658daf029a' # 4, only yahoo, correctly removed unwanted strings in url, need to remove '.help.'
#_hash = 'ee006e5900d899d5b9c460470ae84326' # 4, correctly removed unwanted strings in url, need to remove '.help.'
_hash = 'ffa9b5bc9f71d1fe72cf14b44067e295' # a, unique content_digest

res=aq.load_cached_result(_hash)
res

#%%
########################################
# analyze query
########################################
# summarize
res2=res.groupby('url_host_name').apply(
  lambda x:pd.Series(
    {
      'count':len(x),
      'len_min':int(x['warc_record_length'].min()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_med':int(x['warc_record_length'].median()),
      'len_max':int(x['warc_record_length'].max()),
      'example':x['url'].iloc[0],
    }
  )  
).sort_values('count',ascending=False)
res2[res2['count']>100][:50]

res2[res2['count']>2500]['count'].plot()
res2
# %%

# apply count filter
url_counts = res['url_host_name'].value_counts()
res3 = res[res['url_host_name'].isin(url_counts[url_counts > 2500].index)]

res3.groupby('url_host_name').count().sort_values('url')

res3.groupby('url_host_name').apply(
  lambda x:pd.Series(
    {
      'count':len(x),
      'len_min':int(x['warc_record_length'].min()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_avg':int(x['warc_record_length'].mean()),
      'len_med':int(x['warc_record_length'].median()),
      'len_max':int(x['warc_record_length'].max()),
      'example':x['url'].iloc[0],
    }
  )  
).sort_values('count',ascending=False)

res3.to_pickle('data/finalized/2024-26.pkl')
#%%