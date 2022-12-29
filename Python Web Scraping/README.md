# Python Web Scraping
A Python programming example using the [beautifulsoup](https://www.crummy.com/software/BeautifulSoup/) library to scrape the bookstore catalogue on the [Web Scraping Sandbox](http://books.toscrape.com/) 

## How to use
* Have Python >= 3.9 installed on your machine
* Clone or download this repository
* In a shell, execute the `scrape.py` script with Python 3

## Contents
* __scrape.py__: Script that performs the scraping process and outputs `books.csv`
* __books.csv__: The scraped data extracted by `scrape.py`
* __README.md__: The document you are currently reading

## Background
Predefined datasets aren't always available for the data you need to analyse, sometimes you just have to 'roll your own'. [Web scraping](https://en.wikipedia.org/wiki/Web_scraping) sometimes called web harvesting, or web data extraction is used to extract data and build custom datasets from data publicly available on websites. This project displays proficiency with the tools and techniques required to successfully collect data from the web with Python 3.

### Goals for this project
* Scrape the entire book catalogue with `beautifulsoup`, 1000 pages
* Extract the detailed book data and categories - title, description, category, url, image_url, inventory, rating, price and upc
* Write data out to csv file