from pydantic import BaseModel

class PredictIn(BaseModel):
    Title: str
    Artist: str

class PredictOut(BaseModel):
    Hit: int