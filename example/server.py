from pydantic import BaseModel
from ardennes import Ardennes
from typing import Collection
import asyncio
app = Ardennes()

class JobRequest(BaseModel):
    job_id: int

class JobRequestSection(BaseModel):
    job_id: int
    section_id: int

class JobCompleteSection(BaseModel):
    job_id: int
    section_id: int

class JobComplete(BaseModel):
    job_id: int

@app.scatter(JobRequest, JobRequestSection)
def scatter_job(job: JobRequest) -> Collection[JobRequestSection]:
    return [JobRequestSection(job_id=1, section_id=i) for i in range(10)]

@app.transform(JobRequestSection, JobCompleteSection)
def process_section(section: JobRequestSection) -> JobCompleteSection:
    return JobCompleteSection(job_id=section.job_id, section_id=section.section_id)

@app.gather(JobCompleteSection, JobComplete)
def gather_job(sections: Collection[JobCompleteSection]) -> JobComplete:
    return JobComplete(sections.job)

@app.consume(JobComplete)
def consume_job_complete(job_complete: JobComplete):
    print(f"User function: Got {job_complete}")

async def main():
    await app.start()

if __name__ == "__main__":
    asyncio.run(main())