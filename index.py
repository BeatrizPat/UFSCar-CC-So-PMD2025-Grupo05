import requests
import json
from time import sleep

# Os headers foram pegos da requisição realizada no navegador. Para converter para esse formato
# foi utilizado o site https://curlconverter.com/python/
#
# Caso não seja passado a propriedade timeout na requisição, ela irá demorar bastante para
# finalizar. Quanto menor o timeout, maior a chance da requisição falhar
#

def getOneMovieSongByLetter ():
  headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:139.0) Gecko/20100101 Firefox/139.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=0, i',
  }

  for letter in range(ord('a'), ord('z') + 1):
      letterChar = chr(letter)
      params = { 'letter': letterChar }
      movieListByLetterUrl = f'https://www.what-song.com/_next/data/8uH6pIgXlZMF33aPAsLWO/browse/movies/{letterChar}.json'

      try:
        movieListResp = requests.get(
            movieListByLetterUrl,
            params=params,
            headers=headers,
            timeout=2,
        )
      except requests.ConnectionError as e:
        printf(f"\nUm erro ocorreu ao buscar filmes com a letra {letterChar}")
        continue

      movieListJson = movieListResp.json()

      movieListFirstQuery = movieListJson['pageProps']['dehydratedState']['queries']
      if not hasattr(movieListFirstQuery, "__len__") or len(movieListFirstQuery) <= 0:
        continue
      movieList = movieListFirstQuery[0]['state']['data']['browseMoviesByLetter']
      if not hasattr(movieList, "__len__") or len(movieList) <= 0:
        continue

      firstMovie = movieList[0]
      movieId = firstMovie['id']
      movieName = firstMovie['name']
      movieSlug = firstMovie['slug']
      oneMovieUrl = f'https://www.what-song.com/_next/data/8uH6pIgXlZMF33aPAsLWO/Movies/Soundtrack/{movieId}/{movieSlug}.json'

      try:
        movieResp = requests.get(
            oneMovieUrl,
            params=params,
            headers=headers,
            timeout=2,
        )
      except requests.ConnectionError as e:
        printf(f"\nUm erro ocorreu ao buscar um filme com a letra {letterChar}")
        continue

      movieJson = movieResp.json()

      movieSongs = movieJson['pageProps']['movie']['songs']
      if not hasattr(movieSongs, "__len__") or len(movieSongs) <= 0:
        continue

      firstSong = movieSongs[0]
      songId = firstSong['song']['id']
      songName = firstSong['song']['name']
      spotifyUri = firstSong['song']['spotifyUri']

      print(f"\nExemplo de música de um filme com a letra {letterChar}")
      print(f"\tID: {songId}")
      print(f"\tNome: {songName}")
      print(f"\tFilme: {movieName}")
      if spotifyUri:
        print(f"\tSpotifyUri: {spotifyUri}")
        print(f"\tLink do spotify: https://open.spotify.com/track/{spotifyUri[14:]}")

if __name__ == "__main__":
  getOneMovieSongByLetter()
