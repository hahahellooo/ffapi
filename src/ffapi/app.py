from typing import Union

from fastapi import FastAPI, HTTPException
import pandas as pd

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/sample")
def sample_data():
    df = pd.read_parquet('/home/hahahellooo/code/ffapi/data')
    sample_df = df.sample(n=5)
    r = sample_df.to_dict(orient='records')
    return r


@app.get("/movie/{movie_cd}")#받는 값의 이름과 함수의 이름은 일치해야함
def movie_meta(movie_cd: str):
    df = pd.read_parquet('/home/hahahellooo/code/ffapi/data')
    meta_df = df[df['movieCd'] == movie_cd]
    # 없는 코드를 조회하면  에러가 나서 에러잡는 코드 추가
    if meta_df.empty:
        raise HTTPException(status_code=404, detail="영화를 찾을 수 없습니다.")
    r = meta_df.iloc[0].to_dict()
    return r





