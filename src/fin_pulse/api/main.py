"""API entry point: `uvicorn fin_pulse.api.main:app`."""

from fastapi import FastAPI

from fin_pulse import __version__
from fin_pulse.api.lifespan import lifespan
from fin_pulse.api.routes import router

app = FastAPI(title="fin-pulse", version=__version__, lifespan=lifespan)
app.include_router(router)
