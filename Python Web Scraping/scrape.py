import re
import os
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# __author__ = Martin Hurford
#===============================================================================

class BookScraper:

    """ Class to encapsulate our scraper code.
    Could have been programmed with just functions but this looks nicer """

    def __init__(self, url):
        self.url = url
        self.categories = {}
        self.books      = {}
        self.catalogue  = []


    def run(self):

        """ Execute our methods """

        self._get_category_links_from_home_page()
        self._get_book_links_from_category_pages()
        self._get_book_details()
        return self.catalogue


    def _get_soup(self,url):

        """ Get 'Soup' for the supplied url """

        print('fetching:', url)
        response    = requests.get(url)
        print('status:',response.status_code)
        document    = response.content
        return BeautifulSoup(document,'lxml')


    def _get_category_links_from_home_page(self):

        """ Extract all the category links from the sidebar on the home page """

        soup = self._get_soup(self.url)
        for element in soup.find_all(href=re.compile("/category/books/")): # <a href="catalogue/category/books/travel_2/index.html">Travel</a>
            self.categories.update({element.string.strip() : self.url + element['href']})


    def _get_book_links_from_category_pages(self):

        """ Loop through the category links """

        for category, link in self.categories.items():
            self.book_links = []
            self.books[category] = self._get_book_links_from_category_page(link)
        # We are finished with these dictionaries
        del self.book_links
        del self.categories


    def _get_book_links_from_category_page(self,link):

        """ Extract book detail page links from category pages, recursively run when 'next' link is on page """

        soup = self._get_soup(link)
        for element in soup.select('h3 a'): # <h3><a href="../../../its-only-the-himalayas_981/index.html" title="It's Only the Himalayas">It's Only the Himalayas</a></h3>
            self.book_links.append(urljoin(link,element['href']))

        if not soup.select('li.next a'):  # <li class="next"><a href="page-2.html">next</a></li>
            return self.book_links

        next = soup.select('li.next a')[0]['href']
        next = urljoin(link,next)
        return self._get_book_links_from_category_page(next)


    def _get_book_details(self):

        """ Extract data from book detail pages """

        for category,links in self.books.items():
            for link in links:
                book_detail_page = self._get_soup(link)
                title       = book_detail_page.select_one('h1').string
                description = book_detail_page.select_one('#product_description + p').string if book_detail_page.select_one('#product_description + p') else ''
                image_url   = book_detail_page.select_one('#product_gallery img')['src']
                image_url   = urljoin(link,image_url)
                rating      = book_detail_page.select_one('div.product_main p.star-rating')['class'][1]
                rating      = { 'One' : 1, 'Two' : 2, 'Three' : 3, 'Four' : 4, 'Five' : 5 }[rating]
                upc         = book_detail_page.find('th',string='UPC').find_next_sibling('td').string
                price       = book_detail_page.find('th',string='Price (excl. tax)').find_next_sibling('td').string
                inventory   = book_detail_page.find('th',string='Availability').find_next_sibling('td').string
                inventory   = re.search('(\d+)',inventory).group(0)

                # Build list of dictionarys for our data
                self.catalogue.append(
                {'title'        : title
                ,'image_url'    : image_url
                ,'rating'       : rating
                ,'upc'          : upc
                ,'price'        : price
                ,'inventory'    : inventory
                ,'category'     : category
                ,'url'          : link
                ,'description'  : description[:200] # This is a huge excerpt! Limit to 200 characters.
                })

        # We are finished with this dictionary
        del self.books

#===============================================================================

def main():

    # When executing in VS Code using Anaconda setting the current working directory to the same as this file fixes a few issues
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    output_file = './books.csv'
    home_url    = 'http://books.toscrape.com/'
    scraper     = BookScraper(home_url)
    results     = scraper.run()

    # Write out our extracted data to a cvs file for further analysis
    with open(output_file,'w',encoding='utf-8') as books:
        csv_writer = csv.DictWriter(books,fieldnames=results[0].keys(),dialect='excel',lineterminator='\n')
        csv_writer.writeheader()
        csv_writer.writerows(results)

#===============================================================================

if __name__ == "__main__":
    
    main()
