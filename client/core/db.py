import asyncio
import pickle

# Create the Lock
lock = asyncio.Lock()
database_filename = 'db.pickle'


def _read_db():
    """
    Read the pickle and return the database

    :return: The read database, or an empty dict
    """
    try:
        with open('db.pickle', 'rb') as f:
            return pickle.load(f)
    except:
        return {}


def _write_db(db):
    """
    Writes the database file as a pickle

    :param db: The dict to write to the database file
    :return: Nothing
    """
    with open(database_filename, 'wb') as f:
        pickle.dump(db, f)


async def get_all_jobs():
    """
    Gets all job records for jobs in the database

    :return: An array of all current jobs in the database
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            # Create a new job array
            db['jobs'] = []

        # Return the jobs
        return db['jobs']


async def get_job_by_ui_id(ui_id):
    """
    Gets a job record if one exists for the provided ui id

    :param ui_id: The ui id of the job to look up
    :return: The job details if the job was found otherwise None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Check if the job exists in the database
        if 'jobs' in db:
            for job in db['jobs']:
                if job['ui_id'] == ui_id:
                    # Found the job, return it
                    return job

        # No job matching the criteria was in the database
        return None


async def update_job(new_job):
    """
    Updates a job record in the database if one already exists, otherwise inserts the job in to the database

    :param new_job: The job to update
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            # Create a new job array
            db['jobs'] = []

        # Iterate over the jobs in the database
        found = False
        for job in db['jobs']:
            # Check if this job matches the job being updated
            if job['ui_id'] == new_job['ui_id']:
                # Found the job, update it
                found = True
                job.update(new_job)

        # If no record was found, insert the job
        if not found:
            db['jobs'].append(new_job)

        # Save the database
        _write_db(db)


async def delete_job(job):
    """
    Deletes a job record from the database

    :param job: The job to delete
    :return: None
    """
    # Acquire the lock
    async with lock:
        # Read the database
        db = _read_db()

        # Make sure the database has a jobs entry already
        if 'jobs' not in db:
            return

        # Iterate over the jobs in the database
        for idx in range(len(db['jobs'])):
            # Check if this job matches the job being deleted
            if db['jobs'][idx]['ui_id'] == job['ui_id']:
                # Found the job, delete it
                del db['jobs'][idx]
                break

        # Save the database
        _write_db(db)
