from datetime import datetime

dt = datetime.strptime('2025-11-20', "%Y-%m-%d")
print( dt.replace(hour=23, minute=59, second=59))