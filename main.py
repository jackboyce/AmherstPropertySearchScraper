import selenium.webdriver.support.select
from lxml.html import fromstring
import csv
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import queue
from threading import Thread
import re
from collections import Counter
from cleanco import basename
import regex
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
nltk.download('punkt')
import openpyxl
import PyPDF2 as pypdf
from lxml.etree import tostring
from urllib3.exceptions import MaxRetryError


def wait_for_element_removal(element, timeout_limit):
    element_removed = False
    wait_time = 0.2
    runtime = 0
    while not element_removed:
        try:
            if runtime > timeout_limit:
                raise TimeoutException("Element not removed from page in alloted time")
            temp = element.text
            runtime += wait_time
            time.sleep(wait_time)
        except (NoSuchElementException, StaleElementReferenceException):
            element_removed = True


def inner_html_to_value_list(inner_html):
    parser = fromstring(inner_html)
    ret_prop_list = []
    for j in parser.xpath('//tr')[1:]:
        if len(j) == 9:
            address = j[2].text
            owner = j[3].text
            description = j[5].text
            prop_card = j[4].xpath('./a')[0].get('href')
            ret_prop_list.append([address, owner, description, prop_card])
    return ret_prop_list

# url = "https://gis.amherstma.gov/apps/assessment/PropertySearchInlineRpEmbed.aspx"
file = "rentalpropsstreet"
fields = ['Address', 'Owner', 'Description', 'Prop_Card']
prop_list = []

# Load streets.txt file which contains all of the desired streets
streets = []
with open('streets.txt') as f:
    content = map(lambda x: x.replace('\r', '').replace('\n', ''), f.readlines())
    streets = content

# If you want to load all streets, set streets list to empty
# streets = []
if not streets:
    streets = ['%']

# streets = ['FEARING ST', 'AMITY ST', 'NUTTING AVE', 'MEADOW ST']

class Worker(Thread):
    def __init__(self, request_queue):
        Thread.__init__(self)
        self.queue = request_queue

        self.results = []

    def run(self):
        while True:
            street_from_queue = self.queue.get()
            if street_from_queue == "stop":
                break
            elif street_from_queue == "":
                continue

            url = "https://gis.amherstma.gov/apps/assessment/PropertySearchInlineRpEmbed.aspx"
            table_present = EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolderPanel_GridView1'))

            DRIVER_PATH = 'chromedriver.exe'
            service = Service(DRIVER_PATH)
            options = webdriver.ChromeOptions()
            options.headless = True
            timeout = 5

            driver = webdriver.Chrome(service=service, options=options)
            try:
                driver.get(url)
            except Exception as e:
               print(e)
               driver.quit()
               self.queue.task_done()


            # Select street from streets list
            selenium.webdriver.support.select.Select(
                driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_ddlStreet')).select_by_value(street_from_queue)
            print("Selecting " + street_from_queue)

            # Select the rental checkbox and then press submit
            driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_chkHasRP').click()
            driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_btnSubmit').click()

            # Wait for the properties to load in the first table
            try:
                WebDriverWait(driver, timeout).until(table_present)
            except TimeoutException:
                print("Timeout")

            table_html = driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_GridView1').get_attribute(
                "innerHTML")

            # Process the table html and add value list to final list list
            self.results.append(inner_html_to_value_list(table_html))

            # Load new pages and add data to final list list. Contains final page exception break
            for page_num in range(2, 160):
                print("Loading page " + str(page_num))

                table = driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_GridView1')

                driver.execute_script(
                    "__doPostBack('ctl00$ContentPlaceHolderPanel$GridView1','Page$" + str(page_num) + "')")
                if "Invalid postback or callback argument" in str(driver.title):
                    print("End of pages")
                    break

                try:
                    wait_for_element_removal(table, timeout)
                    WebDriverWait(driver, timeout).until(table_present)
                except TimeoutException:
                    print("Timeout on new pageload " + str(page_num))

                table_html = driver.find_element(By.ID, 'ctl00_ContentPlaceHolderPanel_GridView1').get_attribute(
                    "innerHTML")
                self.results.append(inner_html_to_value_list(table_html))
                time.sleep(0)

            driver.quit()
            self.queue.task_done()


q = queue.Queue()
num_workers = 11

# Add unchecked proxy strings to the queue
for street in streets:
    q.put(street)

# Add the break case to the queue to end the workers
for _ in range(num_workers):
    q.put("stop")

# Run the workers and add them to a list of workers
workers = []
for _ in range(num_workers):
    worker = Worker(q)
    worker.start()
    workers.append(worker)

# Wait for the workers to all be finished before proceeding
for worker in workers:
    worker.join()

# Add the checked results of workers to the checked_proxy_list
for worker in workers:
    for results in worker.results:
        prop_list += results

# Sort the data by owner with most properties
owner_dict = {'': [[]]}

for prop in prop_list:
    if re.sub('[^A-Za-z0-9 ]+', '', prop[1]) not in owner_dict:
        owner_dict[re.sub('[^A-Za-z0-9 ]+', '', prop[1])] = [prop]
    else:
        owner_dict[re.sub('[^A-Za-z0-9 ]+', '', prop[1])].append(prop)

owner_dict.pop('', None)

owner_dict = dict(sorted(owner_dict.items(), key=lambda item: len(item[1]), reverse=True))

veto_list = []
with open('ownerveto.txt') as f:
    content = map(lambda x: re.sub('[^A-Za-z0-9 ]+', '', x.replace('\r', '').replace('\n', '')), f.readlines())
    veto_list = content

for owner in veto_list:
    owner_dict.pop(owner, None)

def sequence_uniqueness(seq, token2frequency):
    return sum(1/token2frequency[t]**0.5 for t in seq)

def name_similarity(a, b, token2frequency):
    a_tokens = set(a)
    b_tokens = set(b)
    a_uniq = sequence_uniqueness(a, token2frequency)
    b_uniq = sequence_uniqueness(b, token2frequency)
    if a_uniq==0 or b_uniq == 0:
        return 0
    else:
        return sequence_uniqueness(a_tokens.intersection(b_tokens), token2frequency)/(a_uniq * b_uniq) ** 0.5

def parse_name(name):
    name = basename(name)
    blacklist = ['PROPERTIES']
    for word in blacklist:
        name = name.replace(word, '')
    #name = name.translate(None, string.punctuation)
    name = regex.sub(r"[[:punct:]]+", "", name)
    tokens = nltk.word_tokenize(name)
    tokens = [t.lower() for t in tokens]
    tokens = [t for t in tokens if t not in stopwords.words('english')]
    return tokens

def build_token2frequency(names):
    alltokens = []
    for tokens in names.values():
        alltokens += tokens

    return Counter(alltokens)

parsed_owners = {owner:parse_name(owner) for owner, data in owner_dict.items()}

token2frequency = build_token2frequency(parsed_owners)

#Generate dict of dicts containing the owner name and comparison scores to all other values
grouping = {}
for merchant, tokens in parsed_owners.items():
    grouping[merchant] = {merchant2: name_similarity(tokens, tokens2, token2frequency) for merchant2, tokens2 in parsed_owners.items()}

# Clean the grouping dict to only include scores between a certain range that have matches
clean_grouping = {'':[]}
for owner, comps in grouping.items():
    # print("Owner: " + owner)
    for owner2, score in comps.items():
        if 0.99 > float(score) > 0.5:
            # print(str(score) + ":\t" + owner2)
            if owner not in clean_grouping:
                clean_grouping[owner] = [owner2]
            else:
                clean_grouping[owner].append(owner2)
clean_grouping.pop('', None)
print(clean_grouping)

skip_list = []

# Move the matching names in the owner_dict list based on the generated matches
for owner, comps in clean_grouping.items():
    if owner in skip_list:
        continue
    for name in comps:
        if name == owner:
            continue
        if name in owner_dict:
            temp = list(owner_dict.pop(name))
            for i in temp:
                owner_dict[owner].append(i)
            # owner_dict[owner].append(temp)
            skip_list.append(name)

print(owner_dict)

# Sort the owner_dict
owner_dict = dict(sorted(owner_dict.items(), key=lambda item: len(item[1]), reverse=True))

prop_list_sorted_by_owner = []
for j in owner_dict.items():
    prop_list_sorted_by_owner += j[1]

print(prop_list_sorted_by_owner)
# Write results to csv file
with open(file + '.csv', "w") as csvfile:
    csvwriter = csv.writer(csvfile)

    csvwriter.writerow(fields)

    csvwriter.writerows(prop_list_sorted_by_owner)

# Write results to xlsx file
df = pd.DataFrame(prop_list, columns=fields)
df.to_excel(file + '.xlsx', index=False)

# proxy_manager = ProxyManager()
# wait_delay = 0
# prop_list = []
#
# for idx in range(1, 10):
#     print("Load: " + str(idx))
#     payload = payload_first + str(idx) + payload_second + view_state + payload_third
#
#     response = requests.request("POST", url, headers=headers, data=payload)
#
#     parser = fromstring(response.text)
#
#     for i in parser.xpath('//table[@id = "ctl00_ContentPlaceHolderPanel_GridView1"]'):
#         for j in i.xpath('//tr')[2:12]:
#
#             address = j[2].text
#             # print(j[2].text)
#             owner = j[3].text
#             # print(j[3].text)
#             prop_card = j[4].xpath('./a')[0].get('href')
#             # print(j[4].xpath('./a')[0].get('href'))
#             prop_list.append([address, owner, prop_card])
#     time.sleep(wait_delay)
#
# for i in prop_list:
#     print(i)
#

# pdfobject=open('4413.pdf', 'rb')
#
# pdf=pypdf.PdfFileReader(pdfobject)
# page = pdf.getPage(0)
# # print(page.extractText())
# print(str(page.mediaBox.getUpperLeft_x()) + ", " + str(page.mediaBox.getUpperLeft_y()))
# print(str(page.mediaBox.getUpperRight_x()) + ", " + str(page.mediaBox.getUpperRight_y()))
# print(str(page.mediaBox.getLowerRight_x()) + ", " + str(page.mediaBox.getLowerRight_y()))
# print(str(page.mediaBox.getLowerLeft_x()) + ", " + str(page.mediaBox.getLowerLeft_y()))
#
# print("crop to upper right: " + str(int(page.mediaBox.getUpperRight_x()/3)) + ", " + str(page.mediaBox.getUpperRight_y()))
# print("crop to lower left: " + str(page.mediaBox.getLowerLeft_x()) + ", " + str(int(page.mediaBox.getUpperRight_y()*(2/3))))
#
# page.cropBox.lowerLeft = (int(page.mediaBox.getUpperRight_x()*(1/42)), int(page.mediaBox.getUpperRight_y()*(9/12)))
# page.cropBox.upperRight = (int(page.mediaBox.getUpperRight_x()*(5/24)), int(page.mediaBox.getUpperRight_y()*(185/200)))
# page.trimBox.lowerLeft = (int(page.mediaBox.getUpperRight_x()*(1/42)), int(page.mediaBox.getUpperRight_y()*(9/12)))
# page.trimBox.upperRight = (int(page.mediaBox.getUpperRight_x()*(5/24)), int(page.mediaBox.getUpperRight_y()*(185/200)))
#
# output = pypdf.PdfFileWriter()
# output.addPage(page)
#
# with open("output.pdf", "wb") as out_f:
#   output.write(out_f)