import bs4
import requests

# Fetch HTML content
url = 'https://finance.yahoo.com/quote/AAPL/profile?p=AAPL'
res = requests.get(url)

# Parse HTML content
soup = bs4.BeautifulSoup(res.text, 'html.parser')

# Extract key executives
executives_section = soup.find_all("td", class_="Ta(start)")

# Extract executive information
executive_rows = executives_section.find_all('div', {'class': 'D(tbr)'})
for row in executive_rows:
    executive_name = row.find('div', {'class': 'D(ib)'}).text
    executive_title = row.find('div', {'class': 'D(tbc)'}).text
    print(executive_name, executive_title)
