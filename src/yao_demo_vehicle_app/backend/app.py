from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .config import conf
from .router import api
from .utils import add_not_found_handler
from .runtime import rt
from .logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info(f"Starting app with configuration:\n{conf.model_dump_json(indent=2)}")
        logger.info(f"Database config: instance_name={conf.db.instance_name}, database_name={conf.db.database_name}, port={conf.db.port}")
        try:
            rt.validate_db()
            rt.initialize_models()
        except Exception as db_error:
            logger.warning(f"Database validation skipped (demo mode): {type(db_error).__name__}: {str(db_error)}")
            logger.info("Running in demo mode without database connection")
        logger.info("App started successfully")
    except Exception as e:
        logger.error(f"Failed to start app: {type(e).__name__}: {str(e)}", exc_info=True)
        raise
    yield


app = FastAPI(title=f"{conf.app_name}", lifespan=lifespan)
ui = StaticFiles(directory=conf.static_assets_path, html=True)

# note the order of includes and mounts!
app.include_router(api)
app.mount("/", ui)


add_not_found_handler(app)
