import redis
import time
import os
import signal

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379))
)

while True:
    try:
        r.ping()
        break
    except redis.exceptions.ConnectionError:
        print("Waiting for Redis...")
        time.sleep(2)

def process_job(job_id):
    print(f"Processing job {job_id}")
    time.sleep(2)  # simulate work
    r.hset(f"job:{job_id}", "status", "completed")
    print(f"Done: {job_id}")

while True:
    try:
        job = r.brpop("job", timeout=5)
        if job:
            _, job_id = job
            process_job(job_id.decode())
    except redis.exceptions.ConnectionError as e:
        print(f"Redis connection lost: {e}, retrying...")
        time.sleep(2)