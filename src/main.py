import asyncio
from viam.module.module import Module
try:
    from models.worker import Worker
except ModuleNotFoundError:
    # when running as local module with run.sh
    from .models.worker import Worker


if __name__ == '__main__':
    asyncio.run(Module.run_from_registry())
