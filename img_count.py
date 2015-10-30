import grequests # asynchronous network requests
from lxml import etree # html parser
import json # json
import time # for careful throttling of requests

## declare constants here

# API_KEY is needed in all RottenTomatoes calls
API_KEY = "hz54u92dhdukkcmxpmyr6rbk"

# RottenTomatoes API
API_URL = "http://api.rottentomatoes.com/api/public/v1.0/lists/movies/in_theaters.json?apikey={0}&page_limit={1}&page={2}"

# these are the IMDB pages we call.

# Image pages contain the information we need but are smaller than title pages,
# so it is better to query those to reduce parsing time.
 
imagePageTemplate = "http://www.imdb.com/title/tt{0}/mediaindex?ref_=tt_pv_mi_sm"

# we call this when constructing our dictionary outputs
titlePageTemplate = "http://www.imdb.com/title/tt{0}"

## declare functions here

def queryRottenTomatoes(page_limit=50,start=1):

    '''
    Retrieves a list of IMDB IDs for each movie currently playing in theaters from the RottenTomatoes API, provided
    the API has them. This function ignores movies with no listed IMDB ID in the RottenTomatoes API, even if it is
    playing in theaters.

    Parameters: page_limit (type Int, sets max results in single page), start (type Int, signals which page number to start from)
    Returns: List of IMDB IDs (type Unicode)
    '''

    # account requests are throttled to a max of 5 per second, so let's push that limit by explicitly choosing max five pages. With the page_limit set to 50, this gives us about 250 results in one iteration max.
    
    assert page_limit <= 50, "Page limits can have maximum value of 50"

    # Generators are fine for grequests.

    requests = (grequests.get(API_URL.format(API_KEY,page_limit,index)) for index in xrange(start,start+4))

    # we send out all requests in parallel
    responses = grequests.map(requests)
    
    # get json output from each requests
    json = map(lambda response: response.json(), responses)

    # only keep pages with actual movies in them - error pages are not for us
    validPages = filter(lambda response: "movies" in response, json)

    # if there are no more valid pages
    if len(validPages) == 0:
        return []

    # retrieve movies from each JSON - each JSON is now replaced by a list of movies
    movies = map(lambda page: page["movies"], validPages)

    # flatten the list - a list of a list of dicts just becomes a list of dicts by chaining them together
    flatMovies = reduce(lambda x,y : x + y, movies)

    # if alternate_ids are present, keep them; otherwise, list them as None
    ids = map(lambda movie: movie.get("alternate_ids",None), flatMovies)

    # do two operations - only keep actual alternate_ids, and then only keep the ones with IMDB in them 
    imdb_ids = map(lambda ID: ID.get('imdb',None),filter(lambda ID: ID is not None, ids))

    # finally, get rid of Nones, and return just a list of raw IMDB ids. 
    result = filter(lambda IMDB_ID: IMDB_ID is not None, imdb_ids)

    # if there are still some results left according to the RT API...
    if int(validPages[0]["total"]) > (start+4)*page_limit:
        new_start = start+5
        # This may exit prematurely if we've exceeded the number of 
        # queries per second, so we must sleep for one second at least
        time.sleep(1)
        result.extend(queryRottenTomatoes(start=new_start))

    # API occasionally returns duplicates of movies in data e.g. if page_limit = 50, then movie Southpaw
    # is on pages 1 and 4, twice. This effect appears to depend strongly on the value of the page_limit :
    # far more duplicates appear to be returned for page_limit = 16 than for page_limit = 50. 
    # This is why the results appear to change when setting different page_limits. To get the most data, 
    # I set the page_limit = 50.

    return set(result) 
    
def getImageCounts(response):

    '''
    
    Parses request response for URL, IMDB_ID and the image counts by using the lxml parser and an Xpath query for our information. Should the information not be there, for one reason or another, we simply pass on it.
    
    Parameters: response (type: requests.Response object)
    Returns: Dictionary of URL (type Unicode/String), IMDB_ID (type Unicode/String), count (type Int, represents image counts)
    '''

    # get IMDB_ID from response.url
    imdb_id = ''.join((character for character in response.url if character.isdigit()))
    
    # get HTML parsed tree
    tree = etree.HTML(response.text)
    
    # get location of desired object using xpath query. Only gets first node that matches this.
    value = tree.xpath('(//*[@id="left"])[1]')

    if value:
        # if there are photos associated with this movie
        return {
        'count': int(value[0].text.split(' ')[-2]), 
        'url': titlePageTemplate.format(imdb_id),
        'imdb_id': imdb_id
        }
    else:
        # if there are no photos associated with it (maybe if page is in development or nobody has uploaded any photos to its page)
        return {'imdb_id': imdb_id, 'url': titlePageTemplate.format(imdb_id), 'count': 0}

def getIMDBPages(ListIDs):

    ''' Returns image counts by querying the relevant IMDB pages. '''

    requests = (grequests.get(imagePageTemplate.format(ID)) for ID in ListIDs)

    # send out requests 30 at a time,  get them back in generator expression.
    responses = grequests.imap(requests, size = 30)

    # get image counts and other information
    data = [getImageCounts(response) for response in responses]

    # serialise result as JSON, and return
    return json.dumps(data)

## main block section

if __name__=='__main__':
    s = queryRottenTomatoes()
    output = getIMDBPages(s)
    print(output)
