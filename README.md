# Scrape RateMyProfessor.com
This tool is meant to collect data from ratemyprofessor.com via webscraping.

Collect professor information by finding a list of schools and then finding all professors associated with each school.  After you have professor data, use the professor ids to find reviews associated with each professor.

# Usage
git clone https://github.com/Lthlnkso/scrape-ratemyprofessor.git
pip install -r requirements.txt
python ScrapeRMP.py --get_profs 0 100 Profs.csv
python ScrapeRMP.py --get_reviews Profs.csv Reviews.csv

Once this is done you will have a CSV of professor data from the first 100 school ids saved in Profs.csv, and a CSV of review data for each of those professors saved in Reviews.csv.

# Maintainance policy
I may or may not maintain this tool.  Depends if it's useful and if ratemyprofessor.com updates a lot making this hard to maintain.
