from bs4 import BeautifulSoup, element
import urllib
import urllib.request
import time
import pandas as pd
import numpy as np


pages = 64
rec_count = 0
rank = []
gname = []
platform = []
year = []
genre = []
critic_score = []
user_score = []
publisher = []
developer = []
sales_na = []
sales_pal = []
sales_jp = []
sales_ot = []
sales_gl = []
#vgchartzscore = []
#shipped = []
# enabling shipped makes all the other variables N / A, so if you want shipped, do it in a separate script standalone
# in that script, see if you can pull in vgchartz score. Not an important field.

max_retries = 5
retry_delay = 5

#Here is the full URL with all fields enabled
#https://www.vgchartz.com/games/games.php?name=&keyword=&console=&region=All&developer=&publisher=&goty_year=&genre=&boxart=Both&banner=Both&ownership=Both&showmultiplat=No&results=50&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0&showpublisher=1&showvgchartzscore=0&showvgchartzscore=1&shownasales=0&shownasales=1&showdeveloper=0&showdeveloper=1&showcriticscore=0&showcriticscore=1&showpalsales=0&showpalsales=1&showreleasedate=0&showreleasedate=1&showuserscore=0&showuserscore=1&showjapansales=0&showjapansales=1&showlastupdate=0&showlastupdate=1&showothersales=0&showothersales=1&showshipped=0&showshipped=1

#Here is the URL broken down into its component pieces
#https://www.vgchartz.com/games/games.php?name=&keyword=
# &console=&region=All&developer=&publisher=&goty_year=&genre=&boxart=Both&banner=Both&ownership=Both
# &results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0
# &showpublisher=1&showvgchartzscore=0
# &showvgchartzscore=1&shownasales=0&shownasales=1
# &showdeveloper=0&showdeveloper=1
# &showcriticscore=0&showcriticscore=1
# &showpalsales=0&showpalsales=1&showreleasedate=0&showreleasedate=1&showuserscore=0&showuserscore=1&showjapansales=0&showjapansales=1
# &showlastupdate=0&showlastupdate=1&showothersales=0&showothersales=1&showshipped=0&showshipped=1

#let's do the above again, but remove anthing that is =0
# https://www.vgchartz.com/gamedb/?page=1&console=&region=All&developer=&publisher=&goty_year=&genre=&boxart=Both&banner=Both&ownership=Both&results=1000&order=Sales&showtotalsales=1&showpublisher=1&showvgchartzscore=1&shownasales=1&showdeveloper=1&showcriticscore=1&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1&showlastupdate=1&showothersales=1&showshipped=1&showgenre=1&sort=GL
#https://www.vgchartz.com/gamedb/?page=
# &console=&region=All&developer=&publisher=&goty_year=&genre=&boxart=Both&banner=Both&ownership=Both
# &results=1000&order=Sales&showtotalsales=1&showpublisher=1
# &showvgchartzscore=1&shownasales=1&showdeveloper=1&showcriticscore=1
# &showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1
# &showlastupdate=1&showothersales=1&showshipped=1
# &showgenre=1&sort=GL



#urlhead = 'http://www.vgchartz.com/gamedb/?page='
# '&console=&region=All&developer=&publisher=&goty_year=&genre=&boxart=Both&banner=Both&ownership=Both'
#urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
# '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
#urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
# &showpublisher=1&showvgchartzscore=0
# &showdeveloper=0&showdeveloper=1
# &showcriticscore=0&showcriticscore=1
#urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
# &showpalsales=0&showpalsales=1&showreleasedate=0&showreleasedate=1&showuserscore=0&showuserscore=1&showjapansales=0&showjapansales=1
#urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
# &showlastupdate=0&showlastupdate=1&showothersales=0&showothersales=1&showshipped=0&showshipped=1&showgenre=1&sort=GL
#urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'


#Initialize the arrays outside of the loop

# after game 2000, I got an error that the index was not the same length as the previous games
# so I saved the data up to that point, and then started again from page 3
# the format looks the same, so I don't know why that error happened.


max_retries = 5
retry_delay = 5

urlhead = 'http://www.vgchartz.com/gamedb/?page='
urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'

for page in range(1, pages):
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
                    #clear temporary arrays before fetching data
                    rank_temp = []
                    gname_temp = []
                    platform_temp = []
                    year_temp = []
                    genre_temp = []
                    critic_score_temp = []
                    user_score_temp = []
                    publisher_temp = []
                    developer_temp = []
                    sales_na_temp = []
                    sales_pal_temp = []
                    sales_jp_temp = []
                    sales_ot_temp = []
                    sales_gl_temp = []
                    # vgchartzscore_temp = []
                    # shipped_temp = []

                    gname_temp.append(" ".join(tag.string.split()))
                    print(f"{rec_count + 1} Fetch data for game {gname_temp[-1]}")

                    data = tag.parent.parent.find_all("td")
                    rank.append(np.int32(data[0].string))
                    platform.append(data[3].find('img').attrs['alt'])
                    publisher.append(data[4].string)
                    developer.append(data[5].string)
                    critic_score.append(
                        float(data[6].string) if
                        not data[6].string.startswith("N/A") else np.nan)
                    user_score.append(
                        float(data[7].string) if
                        not data[7].string.startswith("N/A") else np.nan)
                    sales_na.append(
                        float(data[9].string[:-1]) if
                        not data[9].string.startswith("N/A") else np.nan)
                    sales_pal.append(
                        float(data[10].string[:-1]) if
                        not data[10].string.startswith("N/A") else np.nan)
                    sales_jp.append(
                        float(data[11].string[:-1]) if
                        not data[11].string.startswith("N/A") else np.nan)
                    sales_ot.append(
                        float(data[12].string[:-1]) if
                        not data[12].string.startswith("N/A") else np.nan)
                    sales_gl.append(
                        float(data[8].string[:-1]) if
                        not data[8].string.startswith("N/A") else np.nan)
                    release_year = data[13].string.split()[-1]
                    # different format for year
                    if release_year.startswith('N/A'):
                        year.append('N/A')
                    else:
                        if int(release_year) >= 80:
                            year_to_add = np.int32("19" + release_year)
                        else:
                            year_to_add = np.int32("20" + release_year)
                        year.append(year_to_add)

                    url_to_game = tag.attrs['href']
                    site_raw = urllib.request.urlopen(url_to_game).read()
                    sub_soup = BeautifulSoup(site_raw, "html.parser")
                    h2s = sub_soup.find("div", {"id": "gameGenInfoBox"}).find_all('h2')
                    temp_tag = element.Tag
                    for h2 in h2s:
                        if h2.string == 'Genre':
                            temp_tag = h2
                    genre_temp.append(temp_tag.next_sibling.string)

                    rec_count += 1

                    # Append fetched data to temporary arrays
                    rank_temp.extend(rank)
                    gname_temp.extend(gname)
                    platform_temp.extend(platform)
                    year_temp.extend(year)
                    genre_temp.extend(genre)
                    critic_score_temp.extend(critic_score)
                    user_score_temp.extend(user_score)
                    publisher_temp.extend(publisher)
                    developer_temp.extend(developer)
                    sales_na_temp.extend(sales_na)
                    sales_pal_temp.extend(sales_pal)
                    sales_jp_temp.extend(sales_jp)
                    sales_ot_temp.extend(sales_ot)
                    sales_gl_temp.extend(sales_gl)

                    # Assign temporary arrays to main arrays
                    rank = rank_temp
                    gname = gname_temp
                    platform = platform_temp
                    year = year_temp
                    genre = genre_temp
                    critic_score = critic_score_temp
                    user_score = user_score_temp
                    publisher = publisher_temp
                    developer = developer_temp
                    sales_na = sales_na_temp
                    sales_pal = sales_pal_temp
                    sales_jp = sales_jp_temp
                    sales_ot = sales_ot_temp
                    sales_gl = sales_gl_temp
                   
                   
                    columns = {
                            'Rank': rank,
                            'Name': gname,
                            'Platform': platform,
                            'Year': year,
                            'Genre': genre,
                            'Critic_Score': critic_score,
                            'User_Score': user_score,
                            'Publisher': publisher,
                            'Developer': developer,
                            'NA_Sales': sales_na,
                            'PAL_Sales': sales_pal,
                            'JP_Sales': sales_jp,
                            'Other_Sales': sales_ot,
                            'Global_Sales': sales_gl
                        }
                    print(rec_count)
                    df = pd.DataFrame(columns)
                    print(df.columns)
                    df = df[[
                        'Rank', 'Name', 'Platform', 'Year', 'Genre',
                        'Publisher', 'Developer', 'Critic_Score', 'User_Score',
                        'NA_Sales', 'PAL_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']]
                    df.to_csv("vgsales.csv", sep=",", encoding='utf-8', index=False)

                    break
                except Exception as e:
                    print(f"Error fetching data for game {gname_temp[-1]}: {str(e)}")
                    print(f"Retrying game {gname_temp[-1]}...")
                    retries += 1
                    time.sleep(retry_delay)

            if retries == max_retries:
                print(f"Max retries exceeded for game {gname_temp[-1]}. Skipping...")

    except Exception as e:
        print(f"Error while scraping page {page}: {e}")
        continue

print(f"Total records: {rec_count}")
print(f"Total pages: {pages}")  
print(f"Total games: {len(gname)}")