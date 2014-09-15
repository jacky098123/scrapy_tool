# Scrapy settings for scrapy_tool project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'scrapy_tool'

CONCURRENT_REQUESTS = 5
CONCURRENT_REQUESTS_PER_DOMAIN  = 4

SPIDER_MODULES = ['scrapy_tool.spiders']
NEWSPIDER_MODULE = 'scrapy_tool.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'scrapy_tool (+http://www.yourdomain.com)'

'''
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.redirect.RedirectMiddleware': None, # disable it
}
'''
