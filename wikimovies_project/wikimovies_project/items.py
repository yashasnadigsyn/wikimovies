import scrapy
from itemloaders.processors import TakeFirst, MapCompose, Identity

def clean_text(text):
    """Cleans a single string of text."""
    if text:
        # Removes leading/trailing whitespace and quotes
        cleaned = text.strip().strip('"')
        return cleaned if cleaned else None
    return None

def clean_url(url):
    """Cleans a URL by removing fragment identifiers."""
    if url:
        cleaned = url.split('#')[0].strip()
        return cleaned if cleaned else None
    return None

def clean_year(year):
    """Validates and cleans a year string."""
    if year:
        cleaned = str(year).strip()
        if cleaned and cleaned.isdigit() and len(cleaned) == 4:
            return cleaned
    return None

def clean_table_data(data):
    """Cleans and structures a dictionary of table data."""
    if isinstance(data, dict):
        cleaned_data = {}
        for key, value in data.items():
            if key and value:
                cleaned_key = clean_text(key)
                cleaned_value = clean_text(str(value))
                if cleaned_key and cleaned_value:
                    cleaned_data[cleaned_key] = cleaned_value
        return cleaned_data if cleaned_data else None
    return None

class MovieItem(scrapy.Item):
    title = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=TakeFirst()
    )
    
    movie_url = scrapy.Field(
        input_processor=MapCompose(clean_url),
        output_processor=TakeFirst()
    )
    
    year = scrapy.Field(
        input_processor=MapCompose(clean_year),
        output_processor=TakeFirst()
    )
    
    source_url = scrapy.Field(
        output_processor=TakeFirst()
    )
    
    table_data = scrapy.Field(
        input_processor=MapCompose(clean_table_data),
        output_processor=TakeFirst()
    )
    
    info = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Identity()
    )
    
    plot = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Identity()
    )
    
    cast = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Identity()
    )