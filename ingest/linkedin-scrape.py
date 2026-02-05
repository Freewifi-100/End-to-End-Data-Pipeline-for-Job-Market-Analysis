import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

def split_criteria(criteria_array):
    criteria_dict = {}
    items = []
    for i in criteria_array:
        stripped = i.strip()
        if stripped != "":
            items.append(stripped)
    for index, item in enumerate(items):
        if index % 2 == 0:
            key = item
            value = items[index + 1]
            criteria_dict[key] = value
    return criteria_dict

def request_preview_job_posting(title, location, i):
    list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={title}&location={location}&start={i}".replace(" ", "%2B")
    try:
        response = requests.get(list_url)
    except:
        # sleep
        try:
            time.sleep(5)
            response = requests.get(list_url)
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve data from {list_url}: {e}")
            return None
    list_data = response.text
    list_soup = BeautifulSoup(list_data, 'html.parser')
    page_jobs = list_soup.find_all('li')
    return page_jobs

def extract_job_ids(page_jobs):
    id_list = []
    for job in page_jobs:
        base_card_div = job.find("div", {"class" : "base-card"})
        if base_card_div:
            # print(base_card_div)
            job_id = base_card_div.get("data-entity-urn", "").split(":")[-1]
                # print(f"Job ID: {job_id}")
            id_list.append(job_id)
    return id_list

def scrape_job_posts(id_list):
    job_list = []
    for job_id in id_list:
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        job_response = requests.get(job_url)
        job_post = {}
        job_soup = BeautifulSoup(job_response.text, 'html.parser')
        job_post['job_id'] = "li-" + job_id
        job_post['job_website'] = job_url.split(".")[1]
        job_post['job_url'] = job_url
        try:
            job_post['job_title'] = job_soup.find("h2", {"class" : "top-card-layout__title"}).text.strip()
        except:
            job_post['job_title'] = None
        try:
            job_post['company_name'] = job_soup.find("a", {"class": "topcard__org-name-link"}).text.strip()
        except:
            job_post['company_name'] = None
        try:
            job_post['company_url'] = job_soup.find("a", {"class": "topcard__org-name-link"})['href']
        except:
            job_post['company_url'] = None
        try:
            job_post['time_posted'] = job_soup.find("span", {"class" : "posted-time-ago__text"}).text.strip()
        except:
            job_post['time_posted'] = None
        try:
            job_post['num_applicants'] = job_soup.find("figcaption", {"class" : "num-applicants__caption"}).text.strip()
        except:
            try:
                job_post['num_applicants'] = job_soup.find("span", {"class" : "num-applicants__caption"}).text.strip()
            except:
                job_post['num_applicants'] = None
        try:
            job_post['location'] = job_soup.find("span", {"class" : "topcard__flavor topcard__flavor--bullet"}).text.strip()
        except:
            job_post['location'] = None
        try:
            job_criteria = job_soup.find("ul", {"class" : "description__job-criteria-list"}).text.strip().split("\n")
            criteria_dict = split_criteria(job_criteria)
            job_post['seniority_level'] = criteria_dict.get('Seniority level', None)
            job_post['employment_type'] = criteria_dict.get('Employment type', None)
            job_post['job_function'] = criteria_dict.get('Job function', None)
            job_post['industries'] = criteria_dict.get('Industries', None)
        except:
            job_post['seniority_level'] = None
            job_post['employment_type'] = None
            job_post['job_function'] = None
            job_post['industries'] = None
        try:
            description = job_soup.find("div", {"class" : "show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden"})
            if description.find_all("strong"):
                for strong_tag in description.find_all("strong"):
                    strong_tag.string = f"<strong>{strong_tag.text}</strong>"
            if description.find_all("ul"):
                for li_tag in description.find_all("li"):
                    li_tag.string = f"<li>{li_tag.text}</li>"

            job_post['job_description'] = description.get_text(separator="\n").strip()
        except:
            job_post['job_description'] = None

        job_list.append(job_post)
    return job_list



# create main function (__main__ )
if __name__ == "__main__":
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H%M%S")

    title = ["Data Engineer", "ML Engineer", "Data Scientist", "Data Analyst", "AI Engineer", "Business Intelligence Analyst", "Data specialist"]
    location = "Bagkok, Thailand"
    job_list_all = []
    for t in title:
        for i in range(0, 1000, 25):
            page_jobs = request_preview_job_posting(t, location, i)
            if page_jobs is None:
                continue
            id_list = extract_job_ids(page_jobs)
            job_list = (scrape_job_posts(id_list))
            job_list_all.extend(job_list)
            print(f"[{t}]Scraped {len(job_list_all)} job posts so far...")
            print(f"[{t}]This is page starting at {i}")
    # save to json and csv
    df = pd.DataFrame(job_list_all)
    df.to_json(f"./raw/linkedin/{timestamp}.json", orient="records", lines=True)
    df.to_csv(f"./raw/linkedin/{timestamp}.csv", index=False)