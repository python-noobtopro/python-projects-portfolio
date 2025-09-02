from fastapi import FastAPI
from pydantic import BaseModel
from headless_captcha_cracker import *

app = FastAPI()


class CaptchaCracker(BaseModel):
    captcha_img_base64_str: str

@app.get("/")
async def status():
    return {"message": "Welcome to Captcha Crack API"}


@app.get("/ping")
async def status():
    return {"status": "200 OK"}


@app.post("/img_str")
async def Solve_Captcha(CaptchaModel: CaptchaCracker):
    response = solve_captcha(CaptchaModel.captcha_img_base64_str)
    result_json = {'result':
        [{
            'string': response[0],
            'total_time(secs)': response[1],
            'message': 'Retry with different image until String matches the Captcha'
        }]
    }
#Status flag
#count of attempts
    return result_json
