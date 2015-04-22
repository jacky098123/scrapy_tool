# Scrapy settings for scrapy_tool project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'BaiduSpider'


SPIDER_MODULES = ['scrapy_tool.spiders']
NEWSPIDER_MODULE = 'scrapy_tool.spiders'

CONCURRENT_REQUESTS = 6
CONCURRENT_REQUESTS_PER_DOMAIN  = 5

RETRY_TIMES         = 4
COOKIES_ENABLED     = False
DOWNLOAD_DELAY      = 1
DOWNLOAD_TIMEOUT    = 8

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'scrapy_tool (+http://www.yourdomain.com)'

'''
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.redirect.RedirectMiddleware': None, # disable it

#    'scrapy_tool._middlewares.kxretry.KxRetryMiddleware': 90,
#    'scrapy.contrib.downloadermiddleware.retry.RetryMiddleware': None, # disable it

#    'scrapy_tool._middlewares.kxrandom_useragent.KxRandomUserAgentMiddleware': 91,
#    'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None, # disable it

#    'scrapy_tool._middlewares.kxrandom_proxy.KxRandomProxyMiddleware': 100,
#    'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': None, # disable it
}
'''
