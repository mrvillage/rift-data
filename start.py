import asyncio
from source.env import SERVER
import source.api  # pylint: disable=unused-import

loop = asyncio.get_event_loop()
SERVER.start()
loop.run_forever()

# NOTE TO SELF: PLEASE BE SURE TO PRUNE DOWN THE SIZE AND FREQUENCY OF QUERIES AS MORE
# COMMANDS AND USE-CASES ARE FLESHED OUT TO ENSURE THAT I'M NOT OVERLOADING IT WITH EXCESS
# CACHING OF MASSIVE AMOUNTS OF DATA EVERY FEW MINUTES, BUT INSTEAD ARE ONLY RETRIEVING THE
# DATA THAT IS ACTUALLY REQUIRED FOR MY SERVICE (WHICH WILL MOST LIKELY BE EVERYTHING IN ALL
# HONESTY, SINCE THEN I CAN PROVIDE FREQUENTLY UPDATED STATS AND WHATNOT)

# Data Dumps
# 1. Request the file from the URL
# 2. Use io.BytesIO to make a file-like object from the bytes content of the response
# 3. Put the file-like object into zipfile.ZipFile()
# 4. Read the file name using zipfile.ZipFile().read()
# 5. Parse the return text as a CSV!
