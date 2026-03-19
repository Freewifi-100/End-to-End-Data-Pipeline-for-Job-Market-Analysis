CREATE TABLE job_postings(
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL,
    job_website VARCHAR(255) NOT NULL,
    search_keyword VARCHAR(255) NOT NULL,
    job_url TEXT NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    company_url TEXT NOT NULL,
    time_posted VARCHAR(255) NOT NULL,
    num_applicants VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    seniority_level VARCHAR(100),
    employment_type VARCHAR(100),
    job_function VARCHAR(255),
    industries VARCHAR(255),
    job_description TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
