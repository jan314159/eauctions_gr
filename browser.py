import random
import time
from tempfile import mkdtemp
from typing import Optional

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


class BlockedError(RuntimeError):
    """Raised when eauction.gr (Imperva/Incapsula) refuses to serve the page."""


class Browser:
    """
    One Chrome session shared by every request of a run.

    eauction.gr first redirects to /en/Pow/Index, where JavaScript solves a proof-of-work puzzle,
    stores a cookie and redirects back. Starting a new Chrome for every page (as before) meant
    solving the puzzle and passing the bot checks again on every single request. Keeping one session
    keeps the cookies, so the challenge is solved once and the following pages load directly.

    Usage:
        with Browser() as browser:
            page = browser.get(url, wait_for_class="AList-BoxContainer")
    """

    def __init__(self,
                 headless: bool = True,
                 timeout: int = 60,
                 delay: float = 2.0,
                 pause_every: int = 25,
                 pause_seconds: float = 60,
                 retries: int = 2,
                 binary_location: Optional[str] = None):
        self.headless = headless
        self.timeout = timeout
        self.delay = delay
        self.pause_every = pause_every
        self.pause_seconds = pause_seconds
        self.retries = retries
        self.binary_location = binary_location

        self.requests_made = 0
        self.driver: Optional[Chrome] = None

    def _start(self) -> None:
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        if self.binary_location:
            options.binary_location = self.binary_location

        self.driver = Chrome(options=options)
        self.driver.set_page_load_timeout(self.timeout)

    def close(self) -> None:
        if self.driver is not None:
            try:
                self.driver.quit()
            except WebDriverException:
                pass
            self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _throttle(self) -> None:
        if self.requests_made == 0:
            return
        if self.pause_every and self.requests_made % self.pause_every == 0:
            print(f"scraped {self.requests_made} pages, sleeping {self.pause_seconds}s")
            time.sleep(self.pause_seconds)
        else:
            time.sleep(self.delay + random.uniform(0, self.delay))

    def _is_blocked(self) -> bool:
        source = self.driver.page_source
        return "Incapsula incident ID" in source or "Request unsuccessful. Incapsula" in source

    def _load(self, url: str, wait_for_class: Optional[str]) -> BeautifulSoup:
        if self.driver is None:
            self._start()

        self.driver.get(url)

        def loaded(driver):
            # the proof-of-work page keeps solving until it redirects back to the requested url
            return ("/Pow/" not in driver.current_url
                    and driver.execute_script("return document.readyState") == "complete")

        WebDriverWait(self.driver, self.timeout, poll_frequency=0.5).until(loaded)

        if self._is_blocked():
            raise BlockedError(f"eauction.gr blocked the request: {url}")

        if wait_for_class is not None:
            # pages are rendered on the server, so this is only a short safety net; a page without the
            # element (e.g. an empty result list) is returned as is instead of failing
            try:
                WebDriverWait(self.driver, 5, poll_frequency=0.5).until(
                    lambda driver: driver.find_elements(By.CLASS_NAME, wait_for_class))
            except TimeoutException:
                print(f"{wait_for_class} not found on {url}")

        print(self.driver.current_url)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def get(self, url: str, wait_for_class: Optional[str] = None) -> BeautifulSoup:
        """Downloads url (solving the PoW page if needed) and waits until wait_for_class is on the page."""
        self._throttle()
        self.requests_made += 1

        for attempt in range(self.retries + 1):
            try:
                return self._load(url, wait_for_class)
            except (TimeoutException, WebDriverException, BlockedError) as e:
                if attempt == self.retries:
                    raise
                wait = self.pause_seconds * (attempt + 1)
                print(f"loading {url} failed ({type(e).__name__}), restarting browser in {wait}s")
                # a fresh session gets a fresh PoW challenge and cookies
                self.close()
                time.sleep(wait)


def add_browser_args(parser) -> None:
    parser.add_argument("--delay", type=float, default=2.0,
                        help="seconds to wait between two requests (plus random jitter of the same size)")
    parser.add_argument("--pause_every", type=int, default=25,
                        help="take a longer pause after this many requests, 0 = never")
    parser.add_argument("--pause_seconds", type=float, default=60,
                        help="length of the longer pause, also the base wait before retrying a failed page")
    parser.add_argument("--timeout", type=int, default=60,
                        help="max seconds to wait for a page (incl. the proof-of-work check) to load")
    parser.add_argument("--show_browser", action="store_true", help="run Chrome with a visible window")


def browser_from_args(args) -> Browser:
    return Browser(headless=not args.show_browser, timeout=args.timeout, delay=args.delay,
                   pause_every=args.pause_every, pause_seconds=args.pause_seconds)
