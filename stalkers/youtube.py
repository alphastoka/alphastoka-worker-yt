import requests, re
from bs4 import BeautifulSoup
import pika
import queue
import json
from langid.langid import LanguageIdentifier, model
# from multiprocessing import Pool
import random

#
# Brain for figuring out stuff
#
class RedBrain:
    # Constructor
    # soup - BeautifulSoup object
    def __init__(self, soup, soupMain):
        self.soup = soup
        self.soupMain = soupMain
        self.lidentifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)

    # get description text
    def getDescription(self):
        return str(self.soup.select(".about-description")[0].text)

    # get channel title
    def getTitle(self):
        return str(self.soup.select(".qualified-channel-title-text a")[0].text)

    # try to decide main language of the channel
    # return tuple - (iso-alpha-2 locale code str, confidence rating between 0 to 1 float)
    def getLanguage(self):
        return self.lidentifier.classify(self.getDescription())

    # get country
    # return str
    def getCountry(self):
        try:
            return str(self.soup.select(".country-inline")[0].text.strip())
        except Exception:
            print("unknown country")
            return "Unknown"

    # get url of channel logo
    # return str
    def getLogoUrl(self):
        #channel-header-profile-image
        try:
            return str(self.soup.select(".channel-header-profile-image")[0].attrs['src'])
        except Exception:
            print("unknown logo")
            return ""

    # return [('0', 'views') , ('555', 'subscribers')]
    def getFollowerAndViewCount(self):
        try:
            raw = str(self.soup.select(".about-stats")[0].text.strip())
            m = re.findall(r'([0-9,]+) (views|subscribers)', raw)
            return m
        except Exception as ex:
            print("unknown follower and view")
            return ""

    @staticmethod
    def getWatchLinks(stx):
        rgx_reflink = r"(/watch\?v=[A-Za-z0-9]+)"
        matches = re.findall(rgx_reflink, stx)
        if not matches:
            return []
        return matches

    def getChannelFromVideoRef(self):
        stx = str(self.soupMain)
        matches = RedBrain.getWatchLinks(stx)
        randomVideoLink = random.choice(matches)

        #go to video of one of parent's ch
        r_video = requests.get('https://www.youtube.com/' + randomVideoLink, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        }, cookies={
            'PREF': 'f1=50000000&f5=30&hl=th-TH'
        })
        chanlist = []
        #get referecne from rhs of parent's ch video
        otherWatchLinks = RedBrain.getWatchLinks(r_video.text)
        #go to each video we are suggested to watch
        for videoLink in otherWatchLinks:
            # print(videoLink)
            #naviate there
            rvideo_newchan = requests.get('https://www.youtube.com' + videoLink, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
            })

            rvideo_newchan_soup = BeautifulSoup(rvideo_newchan.text, 'html.parser')
            # #grab the fucking channel
            chanAnchorElem = rvideo_newchan_soup.select(".yt-user-info a")
            if len(chanAnchorElem) > 0:
                hrefs = chanAnchorElem[0]['href'].split("/")
                channelOwner = hrefs[len(hrefs) - 1]
                # # add te rchan w/ highest priority
                chanlist.append(channelOwner)

        return chanlist


    # get all neighboring nodes reachable from
    # this channel
    def getAllChannelRef(self, soup):
        kNNChan = self.getChannelFromVideoRef()
        stx = str(soup)
        rgx_reflink = r"\"(/user/[^\"]*)\""
        m1 = re.findall(rgx_reflink, stx)
        rgx_reflink = r"\"(/channel/[^\"]*)\""
        m2 = re.findall(rgx_reflink, stx)
        mx = []

        if m1 is not None:
            mx = mx + m1
        if m2 is not None:
            mx = mx + m2

        M = []

        for m in mx:
            k = m.split("/")
            M.append(k[2])
        return M+kNNChan

    # get email if avialable
    def getEmail(self):
        descriptionSection = self.getDescription()
        rgx_email = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
        m = re.search(rgx_email, descriptionSection)
        if m is None:
            return ''
        return m.group(0)

    # get phone number if avaiable
    # return array of phone numbers
    def getPhoneNumber(self):
        descriptionSection = self.getDescription()
        regx = r"([0-9]{2}-[0-9]{7})|([0-9]{3}-[0-9]{3}-[0-9]{4})|([0-9]{10})"
        m = re.search(regx, descriptionSection)
        if m is None:
            return []
        return m.group(0)

def parseChannelByIdOrUser(idOrUser, linkQ, visited):
    if idOrUser in visited:
        raise Exception("Seen this before: " +  idOrUser)
    baseUri = "https://www.youtube.com/user/"
    #determine base url /user or /channel
    r = requests.get(baseUri + idOrUser)
    if(r.status_code != 200):
        baseUri = "https://www.youtube.com/channel/"
    visited[idOrUser] = True
    return parseChannelByUrl(baseUri + idOrUser, linkQ, visited)

def parseChannelByUrl(url, linkQ, visited):
    print("Walking to", url)
    data = {}

    r_about = requests.get(url + "/about", headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
    }, cookies={
        'PREF': 'f1=50000000&f5=30&hl=en-US'
    })

    r_main = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
    }, cookies={
        'PREF': 'f1=50000000&f5=30&hl=en-US'
    })

    aboutPage = BeautifulSoup(r_about.text, 'html.parser')
    mainPage = BeautifulSoup(r_main.text, 'html.parser')
    brain = RedBrain(aboutPage, mainPage)
    #parse
    data['medium'] = 'youtube';
    data['title'] = brain.getTitle()
    data['email'] = brain.getEmail()
    data['phone'] = brain.getPhoneNumber()
    data['description'] = brain.getDescription();
    data['logo_url'] = brain.getLogoUrl()
    fvc = brain.getFollowerAndViewCount()
    data['stats'] = {}
    data['stats']['subscriber_count'] = int(fvc[0][0].replace(",", ""))
    data['stats']['view_count'] = int(fvc[1][0].replace(",", ""))
    data['country'] = brain.getCountry()
    data['language'] = brain.getLanguage()
    data['url'] = url;

    dnode = brain.getAllChannelRef(aboutPage)
    dblq = {}
    for xk in dnode:
        if xk in visited or xk in dblq:
            continue
        print("Inserting", xk)
        dblq[xk] = xk
        linkQ.put(xk, 50)

    return data

class Racist(queue.PriorityQueue):
    def __init__(self):
        queue.PriorityQueue.__init__(self)
        self.counter = 0

    def put(self, item, priority):
        queue.PriorityQueue.put(self, (priority, self.counter, item))
        self.counter += 1

    def get(self, *args, **kwargs):
        _, _, item = queue.PriorityQueue.get(self, *args, **kwargs)
        return item

visited = {}
linkQ  = Racist()

# starting points with racism score
linkQ.put("UCO5rwjHY-jcX-gmzOCSApOQ", 100)
linkQ.put("HEARTROCKERChannel", 100)
linkQ.put("FoodTravelTVChannel", 100)
linkQ.put("UCQ0-okjX18v85QlCAr1GBwQ", 100)
linkQ.put("easycooking", 100)
linkQ.put("VrzoChannel", 100)
linkQ.put("GGTKcastation", 100)
linkQ.put("bomberball", 100)
linkQ.put("UC7rtE7hSTaC8xDf5v_7O1qQ", 100)
linkQ.put("UC0TnoMtL2J9OsXU4jgvO_ag", 100)
linkQ.put("UClshsyv7mLwBxLLfSSwLAFQ", 100)
linkQ.put("llookatgreeeen", 100)
linkQ.put("faharisara", 100)
linkQ.put("ninabeautyworld", 100)

# result = []
while True:
    q = linkQ.get()
    try:
        x = parseChannelByIdOrUser(q, linkQ, visited)
        print("Remaining in Q", linkQ.qsize())
        MLAB_API_KEY = "ucQuRaICqjzsxmtTVyuXp3dxzNheiKmy";
        MLAB_TEMP_COLLECTION = "profilesv3"
        mongoUri = "https://api.mlab.com/api/1/databases/alphastoka/collections/" + MLAB_TEMP_COLLECTION + "/?apiKey=" + MLAB_API_KEY
        r=  requests.post(mongoUri, headers={
            "Content-Type" : "application/json"
            }, data=json.dumps(x))

        print(r.status_code)
    except Exception as ex:
        print(ex)
