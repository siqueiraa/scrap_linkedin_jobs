LinkedIn Job Scraper

This Python script allows you to scrape job details from LinkedIn based on specific keywords and location. It utilizes Selenium for web scraping and SQLite for data storage.

Prerequisites

- Python 3.x
- Selenium
- BeautifulSoup
- SQLite3
- YAML
- Requests
- Langid
- Chrome WebDriver

Install the required packages using pip:

pip install -r requirements.txt

Usage

1. Clone this repository to your local machine:

git clone https://github.com/siqueiraa/scrap_linkedin_jobs.git

2. Navigate to the project directory:

cd linkedin-job-scraper

3. Modify the config.yml file with your LinkedIn credentials.

4. Run the script with the desired arguments:

python main.py --keywords <keywords> --location <location> [--only_remote <True/False>] [--more_recents <True/False>]

Example: python main.py --keywords "data engineer" --location "European Union" --only_remote True --more_recents True

Replace <keywords> with the job title keywords you want to search for and <location> with the location where you want to search for job postings. You can also specify optional arguments only_remote and more_recents to filter remote jobs and prioritize more recent postings, respectively.

5. The script will scrape job details from LinkedIn and store them in a SQLite database named jobs.db.

Script Details

- script.py: Main script file containing the ScrapLinkedin class for scraping job details from LinkedIn.
- config.yml: Configuration file for storing LinkedIn credentials.
- cookies.json: File for storing LinkedIn cookies to avoid repeated logins.
- requirements.txt: List of Python dependencies required for the script.
- README.md: This README file providing instructions on how to use the script.

Contact

For any questions or suggestions, feel free to contact me on LinkedIn: [Rafael Siqueira](https://www.linkedin.com/in/rafael-siqueiraa)


License

This project is licensed under the MIT License - see the LICENSE file for details.
