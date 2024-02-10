import requests
from bs4 import BeautifulSoup
import csv

# Suppress InsecureRequestWarning when verify=False
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scrape_bills(bills_url):
    response = requests.get(bills_url, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    bills_summary = []

    # Assuming each bill is in a <tr> within <tbody>, following the structure of the <thead> provided
    for row in soup.select('tbody tr'):
        cols = row.select('td')
        if len(cols) >= 2:  
        # Extracting bill number and title from the row
            bill_url = cols[0].get_text(strip=True)
            bill_title = cols[1].get_text(strip=True)
            bills_summary.append(f"{bill_url}: {bill_title}")

    return ' | '.join(bills_summary)  # Joining all bill summaries into a single string


def scrape_legislators(url):
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    legislators = []

    for row in soup.select('tr'):
        cols = row.select('td')
        if len(cols) >= 5:  
            district = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            party = cols[3].get_text(strip=True)
            bills_link = cols[4].select_one('a')['href']

            if not bills_link.startswith('http'):
                bills_link = "https://www.cga.ct.gov/" + bills_link

            bills_summary = scrape_bills(bills_link)  # Scrape bills data

            legislators.append([district, name, party, bills_summary])

    return legislators

def main():
    url = 'https://www.cga.ct.gov/asp/menu/hlist.asp'  # Replace with the actual URL
    legislators = scrape_legislators(url)

    with open('ct_legislators.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['District', 'Name', 'Party', 'Bills Summary'])  # Updated header
        writer.writerows(legislators)

if __name__ == '__main__':
    main()
