from datetime import datetime
from meteostat import Point, Daily

start = datetime(2018, 1, 1)
end = datetime(2018, 12, 31)

vancouver = Point(49.2497, -123.1193, 70)

# data = Daily(vancouver, start, end)
# data = data.fetch()

# print(data)

