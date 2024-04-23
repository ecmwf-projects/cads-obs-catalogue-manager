from fastapi import FastAPI

from cdsobs.api_rest.endpoints import router

app = FastAPI(title="cads-obs-app", version="0.1", debug=True)
app.include_router(router)
