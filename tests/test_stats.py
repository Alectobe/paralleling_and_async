from src.stats import CrawlerStats


def test_stats_recording():
    stats = CrawlerStats()
    stats.start()

    stats.record_page("https://example.com", True, 200)
    stats.record_page("https://example.com/about", False, 404)
    stats.record_request_attempt()
    stats.record_storage_saved()
    stats.record_storage_error()
    stats.finish()

    data = stats.to_dict()

    assert data["total_pages"] == 2
    assert data["successful"] == 1
    assert data["failed"] == 1
    assert data["request_attempts"] == 1
    assert data["storage_saved"] == 1
    assert data["storage_errors"] == 1
    assert data["status_codes"]["200"] == 1
    assert data["status_codes"]["404"] == 1