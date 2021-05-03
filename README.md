# pete_berryman_baseball
Pete Berryman's portfolio for sabermetric research and other baseball work.

## Sample Work

### Analytics
- [Competing in the big leagues with or without big money](https://blogs.sas.com/content/sascom/2020/12/10/competing-in-the-big-leagues-with-or-without-big-money/)
   - Article written on SAS Blogs as a demonstration of how SAS's software could be applied to visualize data.
   - Examined...
      - how Major League Baseball teams have allocated their payrolls over the last 20 years
      - how certain spending styles have been more successful than other
      - which position groups have been overvalued/undervalued.
   - Used Python to collect player performance and salary data from Baseball Reference.
   - Created powerful visualizations to create greater understanding of the data and the project's findings.

### Canadian Baseball Network

- [Canadians in College Baseball](https://www.canadianbaseballnetwork.com/canadian-baseball-network-canadians-in-college)
   - [![cbn-scrape-roster-full](https://github.com/peteb206/pete_berryman_baseball/actions/workflows/cbn-scrape-roster-full.yml/badge.svg)](https://github.com/peteb206/pete_berryman_baseball/actions/workflows/cbn-scrape-roster-full.yml)
   - Python web scraper using pandas, requests, BeautifulSoup, json, re, numpy and other packages.
   - Scan NCAA, NJCAA, NAIA, etc. schools' baseball rosters for players whose hometown references Canada or a Canadian city or province.
   - Clean and format data due to differences in each school's website formats.
   - Export results to Google Sheets and display nicely using gspread package.

- [Canadians in College Baseball Stats](https://www.canadianbaseballnetwork.com/canadians-in-college-stats)
   - [![cbn-scrape-stat-full](https://github.com/peteb206/pete_berryman_baseball/actions/workflows/cbn-scrape-stat-full.yml/badge.svg)](https://github.com/peteb206/pete_berryman_baseball/actions/workflows/cbn-scrape-stat-full.yml)
   - Python web scraper using pandas, requests, BeautifulSoup, json, re, numpy and other packages.
   - Locate the season statistics of the players found by the Canadians in College Baseball scraper.
   - Clean and format the data found from the NCAA, NJCAA, DakStats, etc. websites.
   - Export results to Google Sheets and display nicely using gspread package.