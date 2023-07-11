from bs4 import BeautifulSoup
import urllib.request
import time
import pandas as pd
import numpy as np

def fetch_data_from_vgchartz(pages=64, max_retries=5, retry_delay=5):
    rec_count = 0
    records = []
    urlhead = 'http://www.vgchartz.com/gamedb/?page='
    urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
    urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
    urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
    urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
    urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'

    for page in range(18, pages):
        try:
            surl = urlhead + str(page) + urltail
            r = urllib.request.urlopen(surl).read()
            soup = BeautifulSoup(r, features="lxml")
            print(f"Page: {page}")

            game_tags = list(filter(
                lambda x: 'href' in x.attrs and x.attrs['href'].startswith('https://www.vgchartz.com/game/'),
                soup.find_all("a")[10:]
            ))

            for tag in game_tags:
                retries = 0
                while retries < max_retries:
                    try:
                        # Fetch data for a single game
                        game_data = fetch_game_data(tag)
                        records.append(game_data)
                        rec_count += 1
                        print(f"Record {rec_count}: {game_data['Name']}")
                        break
                    except Exception as e:
                        print(f"Error fetching data for game {tag.string}: {str(e)}")
                        print(f"Retrying game {tag.string}...")
                        retries += 1
                        time.sleep(retry_delay)

                if retries == max_retries:
                    print(f"Max retries exceeded for game {tag.string}. Skipping...")

            # Save backup after each page
            df = pd.DataFrame(records)
            df.to_csv(f"vgsales_backup_page_{page}.csv", sep=",", encoding='utf-8', index=False)

        except Exception as e:
            print(f"Error while scraping page {page}: {e}")
            continue

    print(f"Total records: {rec_count}")
    print(f"Total pages: {pages}")  
    return records


def fetch_game_data(tag):
    data = tag.parent.parent.find_all("td")
    record = {
        'Rank': np.int32(data[0].string),
        'Name': " ".join(tag.string.split()),
        'Platform': data[3].find('img').attrs['alt'],
        'Publisher': data[4].string,
        'Developer': data[5].string,
        'Critic_Score': float(data[6].string) if not data[6].string.startswith("N/A") else np.nan,
        'User_Score': float(data[7].string) if not data[7].string.startswith("N/A") else np.nan,
        'NA_Sales': float(data[9].string[:-1]) if not data[9].string.startswith("N/A") else np.nan,
        'PAL_Sales': float(data[10].string[:-1]) if not data[10].string.startswith("N/A") else np.nan,
        'JP_Sales': float(data[11].string[:-1]) if not data[11].string.startswith("N/A") else np.nan,
        'Other_Sales': float(data[12].string[:-1]) if not data[12].string.startswith("N/A") else np.nan,
        'Global_Sales': float(data[8].string[:-1]) if not data[8].string.startswith("N/A") else np.nan,
    }

    release_year = data[13].string.split()[-1]
    if release_year.startswith('N/A'):
        record['Year'] = 'N/A'
    else:
        if int(release_year) >= 80:
            year_to_add = np.int32("19" + release_year)
        else:
            year_to_add = np.int32("20" + release_year)
        record['Year'] = year_to_add

    url_to_game = tag.attrs['href']
    site_raw = urllib.request.urlopen(url_to_game).read()
    sub_soup = BeautifulSoup(site_raw, "html.parser")
    genre_tag = sub_soup.find("div", {"id": "gameGenInfoBox"}).find('h2', string='Genre')
    record['Genre'] = genre_tag.next_sibling.string if genre_tag else "N/A"

    return record

def main():
    records = fetch_data_from_vgchartz()
    df = pd.DataFrame(records)
    df.to_csv("vgsales.csv", sep=",", encoding='utf-8', index=False)

if __name__ == "__main__":
    main()
