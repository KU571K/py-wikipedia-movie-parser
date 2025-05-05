import scrapy
from urllib.parse import urljoin


class MoviesSpider(scrapy.Spider):
    name = "movies"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = [
        "https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"
    ]

    def parse(self, response):
        for link in response.css('div.mw-category a::attr(href)').getall():
            url = urljoin(response.url, link)
            yield scrapy.Request(url, callback=self.parse_movie)

        next_page = response.css('a:contains("Следующая страница")::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_movie(self, response):
        raw_title = response.css('title::text').get()
        title = raw_title.replace(" — Википедия", "").strip() if raw_title else "Без названия"
        info = response.css('table.infobox')

        if not info:
            self.logger.warning(f"[!] Пропущено (без инфобокса): {title}")
            yield {
                'Название': title,
                'Жанр': '',
                'Режиссер': '',
                'Страна': '',
                'Год': ''
            }
            return

        def extract_genre(possible_names):
            for name in possible_names:
                row = info.xpath(f'.//tr[th//*[contains(translate(text(), "ЖАНР", "жанр"), "{name.lower()}")]]')
                data = row.xpath('.//td//text()').getall()
                if data:
                    return ', '.join([d.strip() for d in data if d.strip()])
            return ''
        
        def extract_field(possible_names):
            for name in possible_names:
                data = info.xpath(f'.//tr[th[contains(text(), "{name}")]]/td//text()').getall()
                if data:
                    return ', '.join([d.strip() for d in data if d.strip()])
            return ''

        result = {
            'Название': title,
            'Жанр': extract_genre(["жанр", "жанры"]),
            'Режиссер': extract_field(["Режиссёр", "Режиссеры", "Постановщик"]),
            'Страна': extract_field(["Страна", "Страны"]),
            'Год': extract_field(["Год", "Год выпуска", "Премьера", "Выход фильма"]),
        }

        self.logger.info(f"[+] Обработано: {title}")
        yield result