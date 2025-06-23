import json
import csv
import re
import random
import difflib

def ler_linhas_aleatorias_csv(caminho_arquivo, quantidade=0):
  with open(caminho_arquivo, newline='', encoding='utf-8') as csvfile:
    leitor = list(csv.DictReader(csvfile))
    total_linhas = len(leitor)
    linhas_aleatorias = []

    if quantidade > total_linhas:
      raise ValueError(f"O arquivo possui apenas {total_linhas} linhas de dados.")

    if quantidade > 0:
      linhas_aleatorias = random.sample(leitor, quantidade)
    else:
      linhas_aleatorias = leitor

    return linhas_aleatorias

def exibir_json(data, indent = 4, qtd = -1, name = None):
  if (not name == None):
    print(f"\n====== {name} ======\n")
  print(json.dumps(data[:qtd], indent=indent))

def normalizar(json_data, props):
  normalized = []
  for item in json_data:
    for prop in props:
      if prop in item and not item[prop] == None:
        # Remover caracteres não alfanuméricos e converter para minúscula
        name_prop = f'normalized_{prop}'
        newprop_value = re.sub(r'[^a-zA-Z0-9]', '', item[prop]).lower()

        if newprop_value.startswith("themefrom"): newprop_value.removeprefix("themefrom")
        if newprop_value.endswith("theme"): newprop_value.removesuffix("theme")

        item[name_prop] = newprop_value
    normalized.append(item)
  return normalized

def escrever_em_arquivo(nome_arquivo, dados, formato="json"):
  """
  Função para escrever dados em um arquivo.
  
  :param nome_arquivo: O nome ou caminho do arquivo onde os dados serão salvos.
  :param dados: Os dados a serem escritos no arquivo (pode ser string ou dicionário).
  :param formato: O formato dos dados ("texto" ou "json"). Padrão é "texto".
  """
  with open(nome_arquivo, "w", encoding="utf-8") as arquivo:
    if formato == "json":
      # Escrever dados como JSON
      json.dump(dados, arquivo, ensure_ascii=False, indent=4)
    else:
      # Escrever dados como texto simples
      arquivo.write(str(dados))
  print(f"Dados foram escritos em '{nome_arquivo}' com sucesso!")

def comparar_strings(str1, str2, threshold=0.8):
    # Calcula a similaridade entre as duas strings
    similaridade = difflib.SequenceMatcher(None, str1, str2).ratio()

    # Se a similaridade for maior que o limite definido (threshold), as strings são consideradas iguais
    return similaridade >= threshold

def ler_arquivos(qtd_movies=0, show=False):
  soundtracks = normalizar(
    ler_linhas_aleatorias_csv("./dataset/soundtrack.csv", quantidade=qtd_movies),
    ["song_name", "name"]
  )

  if show:
    exibir_json(soundtracks, name="Soundtrack", qtd=1)

  spotify = normalizar(
    ler_linhas_aleatorias_csv("./dataset/spotify.csv"),
    ["song"]
  )

  if show:
    exibir_json(spotify, name="Spotify", qtd=1)

  movies = normalizar(
    ler_linhas_aleatorias_csv("./dataset/movies.csv"),
    ["title"]
  )

  if show:
    exibir_json(movies, name="Movies", qtd=1)

  return (soundtracks, spotify, movies)

def criar_final(soundtracks, spotify, movies):
  final = []
  counter_success = 0
  counter_error = 0

  for track in soundtracks:
    track_name = track.get("normalized_song_name")
    movie_name = track.get("normalized_name")
    print(f'... {track.get("song_name")} || {track.get("name")}', end='')
    encontrou = False

    for song in spotify:
      if "normalized_song" in song and comparar_strings(track_name, song.get("normalized_song")):
        movie_by_song = {}
        for movie in movies:
          if "normalized_title" in movie and comparar_strings(movie.get("normalized_title"), movie_name):
            movie_by_song = movie
            break

        final.append({
          "soundtrack": track,
          "spotify": song,
          "movie": movie_by_song,
        })

        encontrou = True
        break

    if encontrou:
      counter_success += 1
      print()
    else:
      counter_error += 1
      print('... Not found')

  print(f"Sucesso: {counter_success}; Falha: {counter_error}; Total: {counter_error + counter_success}")
  escrever_em_arquivo("dataset/final.json", final)

def show_key_values(soundtracks, spotify, movies):
  tracks = []
  for track in soundtracks:
    if "song_name" in track and "name" in track:
      tracks.append({"song_name": track["song_name"], "movie_name": track["name"]})
  escrever_em_arquivo("dataset/soundtrack_keys.json", tracks)

  songs = []
  for song in spotify:
    if "song" in song and not song["song"] == None:
      songs.append({"song": song["song"]})
  escrever_em_arquivo("dataset/spotify_keys.json", songs)

  movieList = []
  for movie in movies:
    if "title" in movie and not movie["title"] == None:
      movieList.append({"movie": movie["title"]})
  escrever_em_arquivo("dataset/movies_keys.json", movieList)


'''
A função ler_arquivos lê os três datasets e retorna um json de cada um deles. O
argumento show indica se deve ser exibido o primeiro objeto de cada dataset no
terminal, e qtd_movies define quantos objetos de soundtrack deve ser retornado (
se 0, retorna todos, se diferetente de 0 retorna qtd_movies objetos aleatórios).

A função criar_final gera um novo dataset que juntas os três em um só. O dataset
final é composto por objetos com três propriedades (soundtrack, spotify e movie),
cada objeto tem como valor exatamente o mesmo objeto encontrado no dataset de
nome igual a propriedade, os objetos são relacionados pelo nome da música e do
filme.

show_key_values exibe os diferentes valores para as propriedades name e song
encontrados nos três datasets.


Obs. A função criar_final relaciona os datasets comparando o nome da música/filme
normalizados. Para comparar e normalizar é utilizado as funções comparar_strings()
e normalizar(), ambas definidas nesse arquivo. A modificação de ambas as funções
deve impactar na quantidade de músicas encontradas, além de influenciar na
veracidade das informações relacionadas.
'''
if __name__ == "__main__":
  (soundtracks, spotify, movies) = ler_arquivos(show=True, qtd_movies=0)

  criar_final(soundtracks, spotify, movies)
  # show_key_values(soundtracks, spotify, movies)
