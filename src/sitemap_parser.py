from typing import Optional
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import aiohttp

from src.utils import setup_logger


logger = setup_logger()


class SitemapParser:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        self.session = session
        self.visited_sitemaps = set()

    def set_session(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def fetch_sitemap(self, sitemap_url: str) -> list:
        if sitemap_url in self.visited_sitemaps:
            return []

        self.visited_sitemaps.add(sitemap_url)

        if self.session is None:
            logger.warning("⚠️ SitemapParser: session не установлена")
            return []

        try:
            async with self.session.get(sitemap_url) as response:
                response.raise_for_status()
                xml_text = await response.text()
                return await self._parse_sitemap_xml(xml_text, sitemap_url)
        except Exception as error:
            logger.warning(
                f"⚠️ Ошибка загрузки sitemap {sitemap_url}: {type(error).__name__}: {error}"
            )
            return []

    async def _parse_sitemap_xml(self, xml_text: str, sitemap_url: str) -> list:
        urls = []

        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as error:
            logger.warning(
                f"⚠️ Ошибка парсинга sitemap XML {sitemap_url}: {type(error).__name__}: {error}"
            )
            return []

        namespace = ""
        if root.tag.startswith("{"):
            namespace = root.tag.split("}")[0] + "}"

        if root.tag == f"{namespace}sitemapindex":
            for sitemap_tag in root.findall(f"{namespace}sitemap"):
                loc_tag = sitemap_tag.find(f"{namespace}loc")
                if loc_tag is not None and loc_tag.text:
                    nested_sitemap_url = loc_tag.text.strip()
                    nested_urls = await self.fetch_sitemap(nested_sitemap_url)
                    urls.extend(nested_urls)

        elif root.tag == f"{namespace}urlset":
            for url_tag in root.findall(f"{namespace}url"):
                loc_tag = url_tag.find(f"{namespace}loc")
                if loc_tag is not None and loc_tag.text:
                    urls.append(loc_tag.text.strip())

        return urls

    async def discover_and_fetch(self, base_url: str) -> list:
        parsed = urlparse(base_url)
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
        return await self.fetch_sitemap(sitemap_url)