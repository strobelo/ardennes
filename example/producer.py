import asyncio
from example.worker import JobComplete, JobRequest, app


async def main():
    await app.produce(JobRequest(job_id=69))


if __name__ == "__main__":
    asyncio.run(main())
