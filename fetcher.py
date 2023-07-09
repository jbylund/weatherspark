#!/usr/bin/env python
import argparse
import requests
import bs4


def get_args():
    return vars(argparse.ArgumentParser().parse_args())


def get_random_weather():
    response = requests.get(
        "https://weatherspark.com/random",
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:71.0) Gecko/20100101 Firefox/71.0"},
    )
    soup = bs4.BeautifulSoup(response.content, features="lxml")
    for i_script_tag in soup.find_all("script"):
        i_script_tag.extract()
    for i_ins_tag in soup.find_all("ins", attrs={"class": "adsbygoogle"}):
        i_ins_tag.parent.extract()
    print(soup.prettify())


def main():
    get_args()
    get_random_weather()


if "__main__" == __name__:
    main()
