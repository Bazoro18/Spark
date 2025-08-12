import datetime

def get_routes():
    return [
        ("R1", "Thane", "CSMT", 50),
        ("R2", "Mira Road", "Andheri", 40),
        ("R3", "Virar", "Dahanu", 30)
    ]

def get_tickets():
    now = datetime.datetime.now()
    return [
        ("T1", "P1", now - datetime.timedelta(minutes=30), "R1"),
        ("T2", "P2", now - datetime.timedelta(minutes=25), "R1"),
        ("T3", "P3", now - datetime.timedelta(minutes=10), "R2")
    ]

def get_gps():
    now = datetime.datetime.now()
    return [
        ("B1", "R1", 37.77, -122.41, 25.0, now - datetime.timedelta(minutes=5)),
        ("B2", "R2", 37.80, -122.27, 30.0, now - datetime.timedelta(minutes=3))
    ]
