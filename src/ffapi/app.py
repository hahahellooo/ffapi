from typing import Union
import requests
import os
from fastapi import FastAPI, HTTPException
import pandas as pd

app = FastAPI()

def req2list(movie_cd):
    data = req(movie_cd)
    #print(data)
    l = data['movieInfoResult']['movieInfo']
    return l

def req(movie_cd):
    url = gen_url(movie_cd)
    r = requests.get(url)
    data = r.json()
    return data

def gen_url(movie_cd):
    base_url ="http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    key = get_key()
    url = f"{base_url}?key={key}&movieCd={movie_cd}"
    return url

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/sample")
def sample_data():
    df = pd.read_parquet('/home/hahahellooo/code/ffapi/data')
    sample_df = df.sample(n=5)
    r = sample_df.to_dict(orient='records')
    return r


@app.get("/movie/{movie_cd}")
def movie_meta(movie_cd: str):
    df = pd.read_parquet('/home/hahahellooo/code/ffapi/data')

    # df 에서 movieCd == movie_cd row 를 조회 df[['a'] === b] ...
    meta_df = df[df['movieCd'] == movie_cd]

    #if meta_df.empty:
    #    raise HTTPException(status_code=404, detail="영화를 찾을 수 없습니다")

    # 조회된 데이터를 .to_dict() 로 만들어 아래에서 return
    if meta_df['repNationCd'].isna().any():
        d = req2list(movie_cd)
        nn = d['nations'][0]['nationNm']
        
        if nn != "한국":
            meta_df['repNationCd'] = 'F'
        else:
            meta_df['repNationCd'] = 'K'
    
    r = meta_df.iloc[0].to_dict()
    return r

        
    


