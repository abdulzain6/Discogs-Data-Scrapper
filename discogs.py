import contextlib
import requests
from queue import Queue
from bs4 import BeautifulSoup
import threading
import re
import csv
import random
from lxml.html import fromstring
import argparse

def get_proxy_lst():
	print("Getting proxy list!")
	url = 'https://free-proxy-list.net/anonymous-proxy.html'
	response = requests.get(url)
	parser = fromstring(response.text)
	proxies = []
	for i in parser.xpath('//tbody/tr')[:20]:
		if i.xpath('.//td[7][contains(text(),"yes")]'):
		    proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])

		with contextlib.suppress(Exception):
			t = requests.get("https://www.google.com/", proxies={"http": proxy, "https": proxy}, timeout=5)
			if t.status_code == requests.codes.ok:
			    proxies.append(proxy)
	return proxies


class Scraper():
	def __init__(self, start=1, stop=1000000, use_proxy=False ,proxy_func=get_proxy_lst):
		self.start = start
		self.stop = stop
		self.urls = self.fill_queue_with_urls()
		self.result_lst = []
		self.threads = []
		self.use_proxy = use_proxy
		if use_proxy:
			self.proxies = get_proxy_lst()


	def notify(self, prices: list, url: str, have: float, want: float, country: str, item_condition: str) -> None:
		#url: discogs.com/release/sdfasasfasdf, (cond),(price),(ratio)
		item_condition = item_condition[item_condition.find("(")+1:item_condition.find(")")]
		print(f"{url}, ({item_condition}), ({min(prices)}), ({have/want}), ({country})")

	def export(self, path: str) -> None:
		heading = ["Url", "min_price", "have/want", "country", "item_condition"]
		with open(path, 'w', encoding='UTF8') as f:
		    writer = csv.writer(f)
		    writer.writerow(heading)
		    writer.writerows(self.result_lst)	
		print(f"Data exported to {path}")			


	def fill_queue_with_urls(self) -> Queue:
		queue_to_fill = Queue()
		for i in range(self.start, self.stop + 1):
			queue_to_fill.put(f"https://www.discogs.com/sell/release/{i}?ev=rb&limit=250")
		return queue_to_fill


	def is_data_as_needed(self, want: float, least: float, median: float, prices: list) -> bool:
		cheapest_price = min(prices)
		return want > 200 and median > 20 and cheapest_price < least and least >= 15

	def get_float(self, string: str) -> float:
		return float(re.findall(r"[-+]?(?:\d*\.\d+|\d+)", string.replace(",", ""))[0])

	def scrape_data(self) -> None:
		while self.urls.qsize() > 0:
			url = self.urls.get()
			if not self.use_proxy:
				web_page = requests.get(url).text
			else:
				proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
				web_page = requests.get(url, proxies={"http": proxy, "https": proxy}).text
			if "404! Oh no!" in web_page:
				continue
			try:
				soup = BeautifulSoup(web_page, "html.parser")
				statistics = soup.find("div", class_="section_content toggle_section_content")
				unordered_lists = statistics.find_all("ul")

				upper_half = unordered_lists[0].find_all("a", class_="link_1ctor")
				lower_half = unordered_lists[1].find_all("li")

				want = self.get_float(statistics.find("a", attrs={"id" : "want_num_r"}).text)
				have = self.get_float(statistics.find("ul").find("li").find("a").text)
				country_element = soup.find("td", class_="seller_info").find("ul").find_all("li")[2]
				country_element.find("span").decompose()
				country = country_element.text.replace(" ", "").replace("\n", "")

				least = statistics.find("ul", attrs={"class" : "last"}).find_all("li")[1]
				least_span = least.find("span").decompose()
				least = self.get_float(least.text.replace(" ", "").replace("\n", ""))

				item_condition = soup.find("p", class_="item_condition").find_all("span")[2]
				random_text = item_condition.find("span" ,class_="has-tooltip").decompose()

				item_condition = item_condition.text.replace(" ", "").replace("\n", "")

				med = statistics.find("ul", class_="last").find_all("li")[2]
				heading_text = med.find("span").decompose()
				median = self.get_float(med.text.replace(" ", "").replace("\n", ""))

				price_area = soup.find("tbody")
				prices = price_area.find_all("td", class_="item_price hide_mobile")
				prices = [self.get_float(price.find("span", class_="price").text) for price in prices]

				min_price = min(prices)
			except (IndexError, ValueError, AttributeError, Exception) as e:
				continue




			if self.is_data_as_needed(want, least, median, prices):
				self.notify(prices, url, want, have, country, item_condition)
				self.result_lst.append((url, min_price, have/want, country, item_condition))

	def start_execution(self, threads: int = 10) -> None:
		if self.urls.qsize() < threads:
			threads = self.urls.qsize()
		for _ in range(threads):
			t = threading.Thread(target=self.scrape_data)
			self.threads.append(t)
			t.start()
	
		for thread in self.threads:
			thread.join()
				



def main(start=1, stop=1000000, proxy=False, location="scraped_data.csv") -> None:
	print("Starting Script")
	scraper = Scraper(start, stop, use_proxy=proxy)
	scraper.start_execution()
	scraper.export(location)






if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Program to scrape discogs for good prices')
	parser.add_argument('-p','--proxy', help='Use to enable proxy usage', required=False, action='store_true', default=False)
	parser.add_argument('-s','--start', help='The number to start scraping from', required=False, type=int, default=1)
	parser.add_argument('-e','--end', help='THe number to stop scraping till', required=False, type=int, default=1000000)
	parser.add_argument('-l','--location', help='THe location to store data to', required=False, type=str, default="scraped_data.csv")

	args = vars(parser.parse_args())
	main(args["start"], args["end"], args["proxy"], args["location"])
