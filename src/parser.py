import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.models import ParsedPage
from src.utils import setup_logger


logger = setup_logger()


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> dict:
        page = ParsedPage(url=url)

        try:
            soup = BeautifulSoup(html, "lxml")

            page.title = self._extract_title(soup)
            page.text = self.extract_text(soup)
            page.links = self.extract_links(soup, url)
            page.metadata = self.extract_metadata(soup)
            page.images = self.extract_images(soup, url)
            page.headings = self.extract_headings(soup)
            page.tables = self.extract_tables(soup)
            page.lists = self.extract_lists(soup)

        except Exception as error:
            logger.warning(f"⚠️ Ошибка парсинга HTML для {url}: {type(error).__name__}: {error}")
            page.error = f"{type(error).__name__}: {error}"

        return {
            "url": page.url,
            "title": page.title,
            "text": page.text,
            "links": page.links,
            "metadata": page.metadata,
            "images": page.images,
            "headings": page.headings,
            "tables": page.tables,
            "lists": page.lists,
            "error": page.error,
        }

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        seen = set()

        for tag in soup.find_all("a", href=True):
            href = tag.get("href", "").strip()

            if not href:
                continue

            absolute_url = urljoin(base_url, href)

            if self._is_valid_url(absolute_url) and absolute_url not in seen:
                seen.add(absolute_url)
                links.append(absolute_url)

        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        try:
            if selector:
                elements = soup.select(selector)
                text = " ".join(element.get_text(" ", strip=True) for element in elements)
            else:
                soup_copy = BeautifulSoup(str(soup), "lxml")

                for tag in soup_copy(["script", "style", "noscript"]):
                    tag.decompose()

                text = soup_copy.get_text(separator=" ", strip=True)

            text = " ".join(text.split())
            text = re.sub(r"\s+([.,!?;:])", r"\1", text)

            return text
        except Exception as error:
            logger.warning(f"⚠️ Ошибка извлечения текста: {type(error).__name__}: {error}")
            return ""

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata = {
            "title": self._extract_title(soup),
            "description": "",
            "keywords": "",
            "meta_tags": {}
        }

        description_tag = soup.find("meta", attrs={"name": "description"})
        keywords_tag = soup.find("meta", attrs={"name": "keywords"})

        if description_tag and description_tag.get("content"):
            metadata["description"] = description_tag["content"].strip()

        if keywords_tag and keywords_tag.get("content"):
            metadata["keywords"] = keywords_tag["content"].strip()

        for tag in soup.find_all("meta"):
            name = tag.get("name")
            prop = tag.get("property")
            content = tag.get("content")

            if content:
                key = name or prop
                if key:
                    metadata["meta_tags"][key] = content.strip()

        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = []

        for tag in soup.find_all("img"):
            src = tag.get("src", "").strip()
            alt = tag.get("alt", "").strip()

            if not src:
                continue

            absolute_src = urljoin(base_url, src)

            if self._is_valid_url(absolute_src):
                images.append({
                    "src": absolute_src,
                    "alt": alt
                })

        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        return {
            "h1": [tag.get_text(" ", strip=True) for tag in soup.find_all("h1")],
            "h2": [tag.get_text(" ", strip=True) for tag in soup.find_all("h2")],
            "h3": [tag.get_text(" ", strip=True) for tag in soup.find_all("h3")],
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []

        for table in soup.find_all("table"):
            rows_data = []

            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                row_data = [cell.get_text(" ", strip=True) for cell in cells]

                if row_data:
                    rows_data.append(row_data)

            if rows_data:
                tables.append(rows_data)

        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        results = []

        for ul in soup.find_all("ul"):
            items = [li.get_text(" ", strip=True) for li in ul.find_all("li", recursive=False)]
            results.append({
                "type": "ul",
                "items": items
            })

        for ol in soup.find_all("ol"):
            items = [li.get_text(" ", strip=True) for li in ol.find_all("li", recursive=False)]
            results.append({
                "type": "ol",
                "items": items
            })

        return results

    def _extract_title(self, soup: BeautifulSoup) -> str:
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return ""

    def _is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)