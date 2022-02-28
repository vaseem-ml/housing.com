import requests
import pandas as pd
import csv
import string
import os
from tqdm import tqdm
import argparse




class HousingJobScrapper(object):
    def __init__(self):
        self.page=1
        self.size=20
        self.totalPages = 2
        
        self.cities = pd.read_csv("district_list_with_id.csv")
        self.cities["dtname"] = self.cities["dtname"].str.lower()
        
        
        
        """Checking City CSV exists if not then create one"""
        if not os.path.isfile('cities.csv'):
            print("Creating cities CSV")
            with open('cities.csv', 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = ["name", "id", "cityId", "url", "cityClassification", "image", "products"])
                writer.writeheader()
                
        """Checking Localities CSV if not then create"""
        if not os.path.isfile('localities.csv'):
            print("Creating Location CSV")
            with open('localities.csv', 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = ["id", "name", "city", "city_id", "city_url", "displayType", "type", "subType", "url", "center"])
                writer.writeheader()
                
        """Checking Projects CSV if not then create"""
        if not os.path.isfile('projects.csv'):
            print("Creating Projects CSV")
            with open('projects.csv', 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = ["listingId", "project_name", "coords", "address", "image_url"])
                writer.writeheader()
                
        self.housingCities = pd.read_csv("cities.csv")
        self.localities = pd.read_csv("localities.csv")
        
        
        
    
    def scrape_cities(self):
        cityListUrl="https://mightyzeus.housing.com/api/gql/stale?apiName=CITY_LIST_API&isBot=false&source=web"
        cityListRequestPayload={
            "query": "\n  query($service: String, $category: String) {\n    cityListing(service: $service, category: $category) {\n      otherCities {\n        name\n        id\n        cityId\n        url\n        cityClassification\n        image\n        products\n      }\n    }\n  }\n",
            "variables": "{\"service\":\"buy\",\"category\":\"residential\"}"
        }
        cityListResponse = requests.post(cityListUrl, data=cityListRequestPayload)
        for city in cityListResponse.json()["data"]["cityListing"]["otherCities"]:
            if self.cities[self.cities.dtname==city["name"].lower()].any().any():
                if not self.housingCities[self.housingCities.id==city['id']].any().any():
                    cityData = [value for value in city.values()]
                    with open('cities.csv','a') as fd:
                        writer = csv.writer(fd)
                        writer.writerow(cityData)
        self.housingCities = pd.read_csv("cities.csv")
        
        
    def scrape_localities(self):
        locality_list_url = "https://mightyzeus.housing.com/api/gql/cache-first?apiName=TYPE_AHEAD_API&isBot=false&source=web"


        a2z = string.ascii_lowercase[:26]
        allDict=[]
        a2z = [char for char in a2z]
        for i in a2z:
            allDict.append(i)
            for j in a2z:
                allDict.append(i+j)

        for index, city in self.housingCities.iterrows():
#             if city["name"]=="Jaipur":
            for search in tqdm(allDict):
                request_payload = {
                  "query": "\n  query($searchQuery: SearchQueryInput!, $variant: String) {\n    typeAhead(searchQuery: $searchQuery, variant: $variant) {\n      results {\n        id\n        name\n        displayType\n        type\n        subType\n        url\n        center\n      }\n      defaultUrl\n      isCrossCitySearch\n    }\n  }\n",
                  "variables": "{\"searchQuery\":{\"name\":\""+search+"\",\"service\":\"buy\",\"category\":\"residential\",\"city\":{\"name\":\""+city["name"]+"\",\"id\":\""+city["id"]+"\",\"url\":\""+city["url"]+"\",\"isTierTwo\":null,\"products\":[\"paying_guest\",\"rent\",\"buy\",\"plots\",\"commercial\"]},\"excludeEntities\":[],\"rows\":12},\"variant\":\"default\"}"
                }
                localityResponse = requests.post(locality_list_url, data=request_payload)
                localityJson = localityResponse.json()
                for locality in localityJson["data"]["typeAhead"]["results"]:
                    if locality["displayType"]=="Locality":
                        if not self.localities[self.localities["id"]==locality["id"]].any().any():

                            localitiesData = [locality["id"], locality["name"], city["name"], city["id"], city["url"], locality["displayType"], locality["type"], locality["subType"]]
                            with open('localities.csv','a') as fd:
                                writer = csv.writer(fd)
                                writer.writerow(localitiesData)
                
            print("Locations of "+city["name"]+" has been scraped")
        self.localities = pd.read_csv("localities.csv")
        
    def scrape_projects(self):
#         try:
        for index,locality in self.localities.iterrows():
            # if locality["city"]=="Jaipur":
            polyOverview = "https://mightyzeus.housing.com/api/gql?apiName=LOCALITY_OVERVIEW&isBot=false&source=web"
            request_payload = {
                "query": "\n  query($poly: ID!, $overviewRenamed: Boolean!) {\n    localityGlob(locality: { id: $poly }) {\n      overview(locality: { id: $poly }) {\n        name\n        address\n        coverImage\n        livabilityScore\n        cityDescription: description @include(if: $overviewRenamed)\n        description @skip(if: $overviewRenamed)\n        preciseDesc\n        fullLocalityName\n        videoLinks\n        famousPlaces {\n          name\n          address\n        }\n        localityOverviewUrl\n        establishments {\n          id\n          label\n          description\n          meta\n        }\n        links {\n          url\n          label\n        }\n      }\n    }\n  }\n",
                "variables": "{\"poly\":\""+locality["id"]+"\",\"overviewRenamed\":true}"
            }

            localityOverview = requests.post(polyOverview, data=request_payload)
            length=0
            if localityOverview.json()["data"]["localityGlob"]["overview"]["localityOverviewUrl"]:
                length = len(localityOverview.json()["data"]["localityGlob"]["overview"]["localityOverviewUrl"].split("-"))

            localityHash=""
            if length:
                localityHash = localityOverview.json()["data"]["localityGlob"]["overview"]["localityOverviewUrl"].split("-")[length - 1]

            if localityHash:
                print("localityHas", localityHash)


                while self.page < self.totalPages:
                    propertyDataUrl = "https://mightyzeus.housing.com/api/gql/stale?apiName=SEARCH_RESULTS&isBot=false&source=web"
                    request_payload = {
                        "query": "\n  fragment PR on Property {\n    features {\n      label\n      description\n      id\n    }\n    coverImage {\n      src\n      alt\n      videoUrl\n    }\n    polygonsHash\n    hasAutoVideo\n    imageCount\n    propertyType\n    title\n    subtitle\n    isActiveProperty\n    galleryTitle\n    tracking\n    displayPrice {\n      value\n      displayValue\n      unit\n      deposit\n    }\n    address {\n      address\n      url\n      detailedPropertyAddress {\n        url\n        val\n      }\n      distanceFromEntity\n    }\n    url\n    label\n    badge\n    listingId\n    postedDate\n    originalListingId\n    promotions\n    coords\n    tags\n    furnishingType\n    builtUpArea {\n      value\n      unit\n    }\n    sellerCount\n    meta\n    sellers {\n      ...BS\n      phone {\n        partialValue\n      }\n      isCertifiedAgent\n      sellerTag\n    }\n    emi\n    brands {\n      name\n    }\n    details {\n      sliceViewUrl\n      images {\n        images {\n          src\n          alt\n          aspectRatio\n        }\n      }\n      config {\n        displayAreaType\n        propertyConfig {\n          key\n          label\n          data {\n            id\n            price {\n              value\n              displayValue\n              unit\n            }\n            areaConfig {\n              name\n              areaInfo {\n                value\n                unit\n                displayArea\n              }\n            }\n          }\n        }\n      }\n    }\n    minDistanceLocality {\n      distance\n      name\n    }\n    isAuctionFlat\n    photoUnderReview\n    propertyTags\n    isMyGateCertified\n  }\n  fragment SR on Property {\n    ...PR\n    description {\n      overviewDescription\n      highlights\n    }\n    videoTour {\n      startDate\n      endDate\n      url\n      meetingNumber\n    }\n    highlights\n    brands {\n      name\n      image\n      theme {\n        color\n      }\n    }\n  }\n  fragment BS on User {\n    name\n    id\n    image\n    firmName\n    url\n    type\n    isPrime\n    isPaid\n    designation\n  }\n  fragment DS on User {\n    ...BS\n    stats {\n      label\n      description\n    }\n  }\n  fragment Ad on SearchResults {\n    nearbyProperties {\n      ...SR\n    }\n    promotedProperties {\n      type\n      properties {\n        ...PR\n        videoConnectAvailable\n        micrositeRedirectionURL\n      }\n    }\n    ownerNearbyProperties {\n      ...SR\n    }\n    sellers {\n      ...DS\n      meta\n      description\n      sellerDescription\n      cities {\n        id\n        name\n        image\n      }\n      phone {\n        partialValue\n      }\n    }\n    collections {\n      title\n      subTitle\n      image\n      propertyCount\n      url\n      key\n    }\n    searchEntityDetails {\n      id\n      name\n      address\n      image\n      type\n      url\n      subType\n      city {\n        id\n        name\n        url\n      }\n      displayType\n      polygon {\n        id\n        name\n        polylines\n        center\n        type\n        url\n      }\n      developer {\n        name\n        id\n        url\n        image\n      }\n      stats {\n        label\n        description\n      }\n      amenities\n      localityInfo {\n        description\n        videoLinks\n      }\n      images {\n        type\n        images {\n          type\n          src\n          alt\n          caption\n          tag\n          videoUrl\n          category\n        }\n      }\n    }\n  }\n  query(\n    $pageInfo: PageInfoInput\n    $city: CityInput\n    $hash: String!\n    $service: String!\n    $category: String!\n    $meta: JSON\n    $adReq: Boolean!\n    $bot: Boolean!\n  ) {\n    searchResults(\n      hash: $hash\n      service: $service\n      category: $category\n      city: $city\n      pageInfo: $pageInfo\n      meta: $meta\n    ) {\n      properties {\n        ...SR\n        certifiedDetails {\n          isVerifiedProperty\n          similarPropertyKeys\n          isCertifiedProperty\n        }\n        videoConnectAvailable\n        updatedAt\n        digitour {\n          url\n        }\n        socialUrgency {\n          msg\n        }\n        socialContext {\n          msg\n        }\n      }\n      ...Ad @include(if: $adReq)\n      config {\n        filters\n        pageInfo {\n          totalCount\n          size\n          page\n        }\n        entities {\n          id\n          type\n        }\n      }\n      meta\n      structuredData @include(if: $bot)\n    }\n  }\n",
                        "variables": "{\"hash\":\""+localityHash+"\",\"service\":\"buy\",\"category\":\"residential\",\"city\":{\"name\":\""+locality["city"]+"\",\"id\":\""+locality["city_id"]+"\",\"url\":\""+locality["city_url"]+"\",\"isTierTwo\":null,\"products\":[\"paying_guest\",\"buy\",\"plots\",\"commercial\",\"rent\"]},\"pageInfo\":{\"page\":"+str(self.page)+",\"size\":"+str(self.size)+"},\"meta\":{\"filterMeta\":{},\"url\":\"/in/buy/searches/P35g25stzkhodgj09\",\"shouldModifySearchResults\":true},\"bot\":false,\"adReq\":true}"
                    }
                    propertyResponse = requests.post(propertyDataUrl, data=request_payload)
                    config = propertyResponse.json()["data"]["searchResults"]["config"]["pageInfo"]

                    self.page+=1
                    self.totalPages=round(config["totalCount"]/self.size)


                    print("total", self.totalPages, self.page, self.size)
                    if propertyResponse.json()["data"]["searchResults"]["properties"]:
                        for project in propertyResponse.json()["data"]["searchResults"]["properties"]:
                            projectData = []
#                                 print("listding", project["listingId"])
                            coords = project["coords"]
                            listingId = project["listingId"]
                            name = project["address"]["detailedPropertyAddress"][0]["val"]
                            address = project["address"]["detailedPropertyAddress"][0]["val"]
                            if len(project["address"]["detailedPropertyAddress"])==2:
                                address = project["address"]["detailedPropertyAddress"][1]["val"]
                            image_url = None
                            if project["coverImage"]["src"]:
                                image_url = project["coverImage"]["src"].replace("version", "medium")
                            data = [listingId, name, address, coords, image_url]
                            
                            
                            #checking existing value
                            projectsDf = pd.read_csv("projects.csv")
                            if not projectsDf[projectsDf.listingId==listingId].any().any():
                                with open('projects.csv','a') as fd:
                                    writer = csv.writer(fd)
                                    writer.writerow(data)

            else:
                print("No Data")
                    
            self.page=1
















if __name__ == '__main__':
    scraper = HousingJobScrapper()
    scraper.scrape_cities()
    scraper.scrape_localities()
    scraper.scrape_projects()