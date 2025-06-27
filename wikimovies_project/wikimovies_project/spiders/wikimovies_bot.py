## Import libraries
import scrapy
import re
from scrapy.loader import ItemLoader
from ..items import MovieItem

## Spider Class
class WikimoviesBotSpider(scrapy.Spider):
    name = "wikimovies_bot"
    allowed_domains = ["wikipedia.org"]
    
    def start_requests(self):
        base_url = "https://en.wikipedia.org/wiki/List_of_films:_"
        # To run the full scrape, uncomment the list below
        # suffixes = [
        #     "numbers", "A", "B", "C", "D", "E", "F", "G", "H", "I",
        #     "J–K", "L", "M", "N–O", "P", "Q–R", "S", "T", "U–W", "X–Z"
        # ]

        # Using a single suffix for testing
        suffixes = ["numbers"]
        
        self.logger.info(f"Generating requests for {len(suffixes)} pages.")
        
        for suffix in suffixes:
            url = f"{base_url}{suffix}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error
            )
    
    def extract_year_from_text(self, text):
        year_match = re.search(r'\((\d{4})\)', text)
        return year_match.group(1) if year_match else None
    
    def parse(self, response):
        self.logger.info(f"Successfully fetched and now parsing: {response.url}")
        
        ## Get all li elements that contain movies
        movie_lis = response.xpath(
            '//div[@id="mw-content-text"]//div[contains(@class, "div-col")]//li'
        )
        
        if not movie_lis:
            self.logger.warning(f"No movie list items found on {response.url}")
            return
        
        self.logger.info(f"Found {len(movie_lis)} list items on {response.url}")
        
        for li in movie_lis:
            links = li.xpath('.//a')
            
            if not links:
                continue
            
            ## Pattern 1: Single movie with link
            if len(links) == 1:
                link = links[0]
                title = link.xpath('./text()').get()
                href = link.xpath('./@href').get()
                
                li_text = li.xpath('.//text()').getall()
                full_text = ' '.join(li_text).strip()
                year = self.extract_year_from_text(full_text)
                
                if title and href:
                    movie_url = response.urljoin(href)
                    yield scrapy.Request(
                        url=movie_url,
                        callback=self.parse_movie_details,
                        meta={
                            'title': title.strip(),
                            'year': year,
                            'source_url': response.url,
                            'movie_url': movie_url
                        },
                        errback=self.handle_error
                    )
            
            ## Pattern 2: Multiple movies
            elif len(links) > 1:
                title_element = li.xpath('.//i/text()').get()
                if not title_element:
                    text_nodes = li.xpath('./text() | .//text()[not(parent::a)]').getall()
                    title_text = ' '.join(text_nodes).strip()
                    title_match = re.match(r'^([^:(]+)', title_text)
                    title_element = title_match.group(1).strip() if title_match else None
                
                if title_element:
                    base_title = title_element.strip()
                    
                    for link in links:
                        href = link.xpath('./@href').get()
                        link_text = link.xpath('./text()').get()
                        link_title = link.xpath('./@title').get()
                        
                        if href:
                            year = None
                            if link_text and re.match(r'^\d{4}$', link_text.strip()):
                                year = link_text.strip()
                            elif link_title:
                                year = self.extract_year_from_text(link_title)
                            
                            full_title = base_title
                            if link_title and link_title != base_title:
                                title_from_attr = link_title.replace(' film)', ')').replace('(', ' (')
                                if base_title.lower() not in title_from_attr.lower():
                                    full_title = link_title
                            
                            movie_url = response.urljoin(href)
                            yield scrapy.Request(
                                url=movie_url,
                                callback=self.parse_movie_details,
                                meta={
                                    'title': full_title,
                                    'year': year,
                                    'source_url': response.url,
                                    'movie_url': movie_url
                                },
                                errback=self.handle_error
                            )
    
    def parse_movie_details(self, response):
        """Parse individual movie page to extract detailed information"""
        self.logger.info(f"Parsing movie details from: {response.url}")
        
        # Get basic info from meta
        title = response.meta['title']
        year = response.meta['year']
        source_url = response.meta['source_url']
        movie_url = response.meta['movie_url']
        
        # Extract infobox table data
        table_data = self.extract_table_data(response)
        
        # Extract movie info from p tags after the table
        movie_info = self.extract_movie_info(response)
        
        # Extract movie plot
        movie_plot = self.extract_section_content(response, "Plot")
        
        # Extract cast information
        cast = self.extract_section_content(response, "Cast", list_based=True)
        
        # Create and populate the item
        loader = ItemLoader(item=MovieItem(), response=response)
        loader.add_value('title', title)
        loader.add_value('movie_url', movie_url)
        loader.add_value('year', year)
        loader.add_value('source_url', source_url)
        loader.add_value('table_data', table_data)
        loader.add_value('info', movie_info)
        loader.add_value('plot', movie_plot)
        loader.add_value('cast', cast)
        
        yield loader.load_item()
    
    def extract_table_data(self, response):
        """Extract structured data from the infobox table"""
        table_data = {}
        infobox = response.xpath('//table[contains(@class, "infobox")]')
        if infobox:
            rows = infobox.xpath('.//tr')
            for row in rows:
                header = row.xpath('.//th//text()').getall()
                if row.xpath('.//td//li'):
                    data = row.xpath('.//td//li//text()').getall()
                else:
                    data = row.xpath('.//td//text()').getall()

                if header and data:
                    header_text = ' '.join([h.strip() for h in header if h.strip()])
                    data_text = ' '.join([d.strip() for d in data if d.strip()])
                    if header_text and data_text:
                        table_data[header_text] = data_text
                
                img = row.xpath('.//img/@src').get()
                if img:
                    table_data['image_url'] = response.urljoin(img)
        return table_data if table_data else None

    def extract_movie_info(self, response):
        """
        Extracts the introductory paragraphs before the first section heading.
        """
        info_paragraphs = []
        # Find the infobox table
        infobox = response.xpath('//table[contains(@class, "infobox")]')
        if not infobox:
            self.logger.warning(f"No infobox found on {response.url}, cannot reliably get intro.")
            return None
            
        # Select all following sibling elements until the first heading
        for element in infobox.xpath('./following-sibling::*'):
            # Stop if we hit the first section heading
            if element.xpath('self::h2'):
                break
            # Collect text from paragraph tags
            if element.xpath('self::p'):
                text = ' '.join(element.xpath('.//text()').getall()).strip()
                if text:
                    info_paragraphs.append(text)
        
        return info_paragraphs if info_paragraphs else None

    def extract_section_content(self, response, section_id, list_based=False):
        """
        A generic function to extract content from a specific section like "Plot" or "Cast".
        This version uses a more precise stop condition to avoid breaking prematurely.
        
        :param response: The scrapy response object.
        :param section_id: The ID of the section heading (e.g., "Plot", "Cast").
        :param list_based: If True, extracts from <li> elements. If False, from <p>.
        """
        content = []
        # Find the h2 tag for the section
        section_heading = response.xpath(f'//h2[@id="{section_id}"]')

        if not section_heading:
            self.logger.warning(f"No section with ID '{section_id}' found on {response.url}")
            return None

        # Determine the correct node to start iterating from (the h2 or its parent div)
        parent_div = section_heading.xpath("parent::div[contains(@class, 'mw-heading')]")
        start_node = parent_div if parent_div else section_heading

        # Iterate over all elements that are siblings immediately following the start_node
        for element in start_node.xpath('./following-sibling::*'):
            print(element.get())
            if element.xpath("self::h2 or self::div[contains(@class, 'mw-heading')]"):
                self.logger.info(f"Found next section heading. Stopping extraction for '{section_id}'.")
                break
            
            if list_based:
                # For list-based content like 'Cast'.
                items = element.xpath('.//li')
                if items:
                    for li in items:
                        text = ' '.join(li.xpath('.//text()').getall()).strip()
                        if text:
                            content.append(text)
            else:
                # For paragraph-based content like 'Plot'
                if element.xpath('self::p'):
                    text = ' '.join(element.xpath('.//text()').getall()).strip()
                    if text:
                        content.append(text)

        if not content:
            self.logger.warning(f"Extracted no content for section '{section_id}' on {response.url}")

        return content if content else None
    
    def handle_error(self, failure):
        request = failure.request
        self.logger.error(f"Request to {request.url} failed")
        self.logger.error(f"Failure type: {failure.type}")
        self.logger.error(f"Failure value: {failure.value}")
        yield {
            'error': f"Failed to fetch {request.url}",
            'failure_type': str(failure.type),
            'details': str(failure.value),
        }