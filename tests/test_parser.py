import pytest
from bs4 import BeautifulSoup

from src.parser import HTMLParser


@pytest.fixture
def parser():
    return HTMLParser()


@pytest.fixture
def sample_html():
    return """
    <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="Test description">
            <meta name="keywords" content="python,async,parser">
        </head>
        <body>
            <h1>Main title</h1>
            <h2>Subtitle</h2>
            <h3>Small heading</h3>

            <p>Hello <b>world</b>!</p>

            <a href="/about">About</a>
            <a href="https://example.org/contact">Contact</a>

            <img src="/image.png" alt="Test image">

            <ul>
                <li>Item 1</li>
                <li>Item 2</li>
            </ul>

            <ol>
                <li>First</li>
                <li>Second</li>
            </ol>

            <table>
                <tr>
                    <th>Name</th>
                    <th>Age</th>
                </tr>
                <tr>
                    <td>Alice</td>
                    <td>25</td>
                </tr>
            </table>
        </body>
    </html>
    """


@pytest.mark.asyncio
async def test_parse_html_valid(parser, sample_html):
    result = await parser.parse_html(sample_html, "https://example.com")

    assert result["url"] == "https://example.com"
    assert result["title"] == "Test Page"
    assert "Hello world!" in result["text"]
    assert len(result["links"]) == 2
    assert result["metadata"]["description"] == "Test description"


def test_extract_links_with_relative_urls(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    links = parser.extract_links(soup, "https://example.com")

    assert "https://example.com/about" in links
    assert "https://example.org/contact" in links


def test_extract_text(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    text = parser.extract_text(soup)

    assert "Hello world!" in text
    assert "Main title" in text


def test_extract_metadata(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    metadata = parser.extract_metadata(soup)

    assert metadata["title"] == "Test Page"
    assert metadata["description"] == "Test description"
    assert metadata["keywords"] == "python,async,parser"


def test_extract_images(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    images = parser.extract_images(soup, "https://example.com")

    assert len(images) == 1
    assert images[0]["src"] == "https://example.com/image.png"
    assert images[0]["alt"] == "Test image"


def test_extract_headings(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    headings = parser.extract_headings(soup)

    assert headings["h1"] == ["Main title"]
    assert headings["h2"] == ["Subtitle"]
    assert headings["h3"] == ["Small heading"]


def test_extract_tables(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    tables = parser.extract_tables(soup)

    assert len(tables) == 1
    assert tables[0][0] == ["Name", "Age"]
    assert tables[0][1] == ["Alice", "25"]


def test_extract_lists(parser, sample_html):
    soup = BeautifulSoup(sample_html, "lxml")
    lists_data = parser.extract_lists(soup)

    assert len(lists_data) == 2
    assert lists_data[0]["type"] == "ul"
    assert lists_data[0]["items"] == ["Item 1", "Item 2"]
    assert lists_data[1]["type"] == "ol"
    assert lists_data[1]["items"] == ["First", "Second"]


@pytest.mark.asyncio
async def test_parse_broken_html(parser):
    broken_html = "<html><head><title>Broken<title><body><h1>Test"
    result = await parser.parse_html(broken_html, "https://example.com")

    assert result["url"] == "https://example.com"
    assert "title" in result
    assert "text" in result