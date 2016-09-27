import requests, re
from bs4 import BeautifulSoup
import pika
import json
from langid.langid import LanguageIdentifier, model
# from multiprocessing import Pool
import random
import time

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
        try:
            return str(self.soup.select(".about-description")[0].text)
        except Exception:
            return ""
            
    # get channel title
    def getTitle(self):
        try:
            return str(self.soup.select(".qualified-channel-title-text a")[0].text)
        except Exception:
            return ""

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
            print("country not specified")
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
        # start = time.time()
        stx = str(self.soupMain)
        matches = RedBrain.getWatchLinks(stx)
        if len(matches) == 0:
            return []
        
        randomVideoLink = random.choice(matches)

        #go to video of one of parent's ch
        r_video = requests.get('https://www.youtube.com/' + randomVideoLink, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36',
        }, cookies={
            'PREF': 'f1=50000000&f5=30&hl=th-TH'
        })

        chanlist = []

        #branded-page-related-channels
        # r_video_soup = BeautifulSoup(r_video.text, 'html.parser')

        #get referecne from rhs of parent's ch video
        otherWatchLinks = list(set(RedBrain.getWatchLinks(r_video.text)))
        #go to each video we are suggested to watch

        # print("[TIME] getChannelFromVideoRef: ",time.time() - start)
        start = time.time()
        
        print("len(otherWatchLinks)" ,len(otherWatchLinks))
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
        
        print("[TIME] getChannelFromVideoRef (otherWatchLinks): ",time.time() - start)
        return chanlist


    # get all neighboring nodes reachable from
    # this channel
    def getAllChannelRef(self, soup):
        # kNNChan = self.getChannelFromVideoRef()
        stx = str( soup.select(".branded-page-related-channels-list"))
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
        
        print("M=", M)
        return M

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

#
# Parse Stuff
#
class Parser:
    def __init__(self):
        pass

    def parseChannelByIdOrUser(self, idOrUser, findkNN):
        baseUri = "https://www.youtube.com/user/"
        #determine base url /user or /channel
        r = requests.get(baseUri + idOrUser)
        if(r.status_code != 200):
            baseUri = "https://www.youtube.com/channel/"
        
        return self.parseChannelByUrl(baseUri + idOrUser, idOrUser, findkNN)

    #
    # Returns ( Object<Channel>, List<Neighbor>)
    #
    def parseChannelByUrl(self, url, idOrUser, findkNN):
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
        print("TITLE: ", brain.getTitle())
        #parse
        data['medium'] = 'youtube';
        data['title'] = brain.getTitle()
        data['email'] = brain.getEmail()
        data['phone'] = brain.getPhoneNumber()
        data['description'] = brain.getDescription();
        data['logo_url'] = brain.getLogoUrl()
        fvc = brain.getFollowerAndViewCount()
        data['stats'] = {}
        try:
            data['stats']['subscriber_count'] = int(fvc[0][0].replace(",", ""))
            data['stats']['view_count'] = int(fvc[1][0].replace(",", ""))
        except Exception:
            data['stats']['subscriber_count'] = 0
            data['stats']['view_count'] = 0

        data['country'] = brain.getCountry()
        data['language'] = brain.getLanguage()
        data['url'] = url;
        data['id'] = idOrUser

        print("[x] getting relations")
        dnode = []
        if findkNN:
            pass
        dnode = brain.getAllChannelRef(aboutPage)
        print("[x] Found", len(dnode), "relations")
        
        return (data, list(set(dnode)))

class Queue():
    def __init__(self):
        self.q = []

    def put(self, item):
        self.q.push(item)

    def get(self, *args, **kwargs):
        return self.p.pop(0)