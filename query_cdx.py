# TODO THIS WORKS!
# pip install cdx_toolkit
# https://github.com/cocrawler/cdx_toolkit/


########################################################
# command line
########################################################
'''
cdxt --help
cdxt iter --help
cdxt warc --help
cdxt size --help
cdxt iter --cc --from 202406 --to 202407 'commoncrawl.org/*'
cdxt size --url 'commoncrawl.org/*'
'''

#%%
########################################################
# python
########################################################
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
cdx.customize_index_list({'from_ts':'202407','to':'202407'}) # https://index.commoncrawl.org/collinfo.json
url = 'commoncrawl.org/*'

# https://skeptric.com/searching-100b-pages-cdx/
# https://github.com/cocrawler/cdx_toolkit/blob/4f77e7ed79a267a3ae1aa6026e58556435ab6be1/cdx_toolkit/__init__.py#L203
'''
    def get(self, url, **kwargs):
        # from_ts=None, to=None, matchType=None, limit=None, sort=None, closest=None,
        # filter=None, fl=None, page=None, pageSize=None, showNumPages=None):
'''
cdx.get(url = 'yahoo.com')


####################################################
# WORKS
####################################################
LIMIT = 1e8
LIMIT = 10
url = '*.hk01.com'
cdx = cdx_toolkit.CDXFetcher(source='cc')
objs = list(cdx.iter(url, from_ts='202406', to='202407',  limit=LIMIT, filter='=status:200'))
[o.data for o in objs]



filter
dir(cdx)
#%%

print(url, 'size estimate', cdx.get_size_estimate(url))

for obj in cdx.iter(url, limit=1):
    print(obj)

print(obj.content)

####################################################
# https://skeptric.com/searching-100b-pages-cdx/
####################################################

#%%
#############
import requests
import json

url = '*.hk01.com'
cdx_indexes = requests.get('https://index.commoncrawl.org/collinfo.json').json()
api_url = cdx_indexes[0]['cdx-api']
#%%

# filter options, ~=!!=!~
# https://github.com/webrecorder/pywb/wiki/CDX-Server-API#filter

r = requests.get(api_url,
                 params = {
                     'url': url,
                     'limit': 10,
                     'output': 'json',
                     #'fl': 'url,filename,offset,length',
                     'filter': ['=status:200', # exact match: field "mime" is "text/html"
                                '=mime-detected:text/html', # exact match: field "mime" is "text/html"
								'~languages:zho', # regex match: expression matches beginning of field "mime" (cf. re.match)
                                #'~url:.*/comments/',
								]
                 })
records = [json.loads(line) for line in r.text.split('\n') if line]



'''
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE (crawl = 'CC-MAIN-2024-26')
  AND subset = 'warc'
  AND url_host_tld = 'com'
  AND content_languages = 'zho'
  AND fetch_status = 200
  AND url LIKE '%hk%'
  AND url_host_registered_domain = 'yahoo.com'
'''