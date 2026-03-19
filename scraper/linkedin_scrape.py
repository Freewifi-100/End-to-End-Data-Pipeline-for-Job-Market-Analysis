import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

REQUEST_TIMEOUT = 10
RETRY_SLEEP_SECONDS = 5
MAX_RETRIES = 2


def split_criteria(criteria_array):
    criteria_dict = {}
    items = []
    for item in criteria_array:
        stripped = item.strip()
        if stripped:
            items.append(stripped)
    for index in range(0, len(items), 2):
        if index + 1 < len(items):
            criteria_dict[items[index]] = items[index + 1]
    return criteria_dict


def request_with_retry(url, max_retries=MAX_RETRIES):
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            return requests.get(url, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(RETRY_SLEEP_SECONDS)
    print(f"Failed to retrieve data from {url}: {last_error}")
    return None

def request_preview_job_posting(title, location, i):
    list_url = (
        f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={title}&location={location}&start={i}"
    ).replace(" ", "%2B")
    response = request_with_retry(list_url)
    if response is None:
        return None
    list_soup = BeautifulSoup(response.text, "html.parser")
    return list_soup.find_all("li")

def extract_job_ids(page_jobs):
    id_list = []
    for job in page_jobs:
        base_card_div = job.find("div", {"class": "base-card"})
        if base_card_div:
            job_id = base_card_div.get("data-entity-urn", "").split(":")[-1]
            id_list.append(job_id)
    return id_list

def parse_job_details(job_soup):
    job_details = {}
    
    # Parse job title
    job_title_tag = job_soup.find("h2", {"class": "top-card-layout__title"})
    if job_title_tag is None:
        job_title_tag = job_soup.find("h1", {"class": "top-card-layout__title"})
    job_details["job_title"] = job_title_tag.text.strip() if job_title_tag else None

    # Parse company name
    company_name_tag = job_soup.find("a", {"class": "topcard__org-name-link"})
    if company_name_tag is None:
        company_name_tag = job_soup.find(
            "span", {"class": "topcard__flavor topcard__flavor--black-link"}
        )
    job_details["company_name"] = (
        company_name_tag.text.strip() if company_name_tag else None
    )

    # Parse company URL
    company_url_tag = job_soup.find("a", {"class": "topcard__org-name-link"})
    job_details["company_url"] = (
        company_url_tag["href"] if company_url_tag and company_url_tag.has_attr("href") else None
    )

    # Parse time posted
    time_posted_tag = job_soup.find("span", {"class": "posted-time-ago__text"})
    job_details["time_posted"] = time_posted_tag.text.strip() if time_posted_tag else None

    # Parse number of applicants
    applicants_tag = job_soup.find("figcaption", {"class": "num-applicants__caption"})
    if applicants_tag is None:
        applicants_tag = job_soup.find("span", {"class": "num-applicants__caption"})
    job_details["num_applicants"] = applicants_tag.text.strip() if applicants_tag else None

    # Parse location
    location_tag = job_soup.find(
        "span", {"class": "topcard__flavor topcard__flavor--bullet"}
    )
    job_details["location"] = location_tag.text.strip() if location_tag else None

    # Parse job criteria (seniority, employment type, etc.)
    criteria_tag = job_soup.find("ul", {"class": "description__job-criteria-list"})
    if criteria_tag:
        job_criteria = criteria_tag.text.strip().split("\n")
        criteria_dict = split_criteria(job_criteria)
        job_details["seniority_level"] = criteria_dict.get("Seniority level")
        job_details["employment_type"] = criteria_dict.get("Employment type")
        job_details["job_function"] = criteria_dict.get("Job function")
        job_details["industries"] = criteria_dict.get("Industries")
    else:
        job_details["seniority_level"] = None
        job_details["employment_type"] = None
        job_details["job_function"] = None
        job_details["industries"] = None

    # Parse job description
    description = job_soup.find(
        "div",
        {
            "class": ("show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden")
        },
    )
    if description:
        for strong_tag in description.find_all("strong"):
            strong_tag.string = f"<strong>{strong_tag.text}</strong>"
        for li_tag in description.find_all("li"):
            li_tag.string = f"<li>{li_tag.text}</li>"
        job_details["job_description"] = description.get_text(separator="\n").strip()
    else:
        job_details["job_description"] = None

    return job_details


def scrape_job_posts(id_list, search_keyword):
    job_list = []
    for job_id in id_list:
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        job_response = request_with_retry(job_url)
        if job_response is None:
            continue

        # Create base job post with metadata
        job_post = {
            "job_id": f"li-{job_id}",
            "job_website": job_url.split(".")[1],
            "search_keyword": search_keyword,
            "job_url": job_url,
        }
        
        # Parse detailed job information
        job_soup = BeautifulSoup(job_response.text, "html.parser")
        job_details = parse_job_details(job_soup)
        
        # Merge details into job post
        job_post.update(job_details)
        job_list.append(job_post)
        
    return job_list

def final_check_job_ids(df, df_null):
    for index, row in df_null.iterrows():
        response = request_with_retry(row['job_url'])
        soup = BeautifulSoup(response.text, 'html.parser')
        job_details = parse_job_details(soup)
        # update in same df
        df.at[index, 'job_title'] = job_details['job_title']
        df.at[index, 'company_name'] = job_details['company_name']
        df.at[index, 'company_url'] = job_details['company_url']
        df.at[index, 'time_posted'] = job_details['time_posted']
        df.at[index, 'num_applicants'] = job_details['num_applicants']
        df.at[index, 'location'] = job_details['location']
        df.at[index, 'seniority_level'] = job_details['seniority_level']
        df.at[index, 'employment_type'] = job_details['employment_type']
        df.at[index, 'job_function'] = job_details['job_function']
        df.at[index, 'industries'] = job_details['industries']
        df.at[index, 'job_description'] = job_details['job_description']
    
def scrape():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H%M%S")

    title = [
        "Data Engineer",
        "ML Engineer",
        "Data Scientist",
        "Data Analyst",
        "AI Engineer",
        "Business Intelligence Analyst",
        "Data specialist",
    ]
    location = "Bagkok, Thailand"
    job_list_all = []
    for t in title:
        for i in range(0, 1000, 25):
            page_jobs = request_preview_job_posting(t, location, i)
            if page_jobs is None:
                continue
            id_list = extract_job_ids(page_jobs)
            job_list = scrape_job_posts(id_list, t)
            job_list_all.extend(job_list)
            print(f"[{t}]Scraped {len(job_list_all)} job posts so far...")
            print(f"[{t}]This is page starting at {i}")
    # save to json and csv
    df = pd.DataFrame(job_list_all)
    
    null_title_jobs = df[df['job_title'].isnull()]
    print(f"Number of job posts with null title: {len(null_title_jobs)}")
    while not null_title_jobs.empty or not df[df['industries'].isnull()].empty:
        final_check_job_ids(df, null_title_jobs)
        null_title_jobs = df[df['job_title'].isnull()]
        print (f"After final check, number of job posts with null title: {len(null_title_jobs)}")

    # add last column scraped_at
    df['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_json(f"./raw/linkedin/{timestamp}.json", orient="records", lines=True)
    df.to_csv(f"./raw/linkedin/{timestamp}.csv", index=False)

if __name__ == "__main__":    
    scrape()