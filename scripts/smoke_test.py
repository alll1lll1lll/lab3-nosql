#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--base", default="http://localhost:8080")
ap.add_argument("--admin", default="100000")
args = ap.parse_args()
BASE, ADMIN = args.base.rstrip("/"), args.admin
FAILED = []

def call(method, path, person=None, body=None, params=None, expect=None):
    url = BASE + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if person:
        req.add_header("Person-Id", person)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    payload = json.loads(raw) if raw else None
    if expect is not None and status != expect:
        sys.exit(f"{method} {path} -> {status}, ожидали {expect}: {payload}")
    return status, payload

def check(name, ok, details=""):
    print(("  OK   " if ok else "  FAIL ") + name + (f"  ({details})" if details else ""))
    if not ok:
        FAILED.append(name)

def eventually(fn, timeout=10.0):
    deadline = time.time() + timeout
    while True:
        result = fn()
        if result or time.time() > deadline:
            return result
        time.sleep(0.3)

started = dt.datetime.now().astimezone() - dt.timedelta(seconds=2)

print("1. CRUD лабораторных 1-2")
_, person = call("POST", "/api/persons", body={"lastName": "Тестов", "firstName": "Иван",
                                               "middleName": "Петрович", "role": "STUDENT"}, expect=201)
pid = person["isuId"]
_, rooms = call("GET", "/api/rooms", person=pid, expect=200)
room = next(r for r in rooms if not r["teacherOnly"] and r["capacity"] >= 2)
_, cats = call("GET", "/api/categories", person=pid, expect=200)
day = (dt.date.today() + dt.timedelta(days=30 + int(time.time()) % 200)).isoformat()
bookings = []
for i in range(3):
    _, b = call("POST", "/api/bookings", person=pid, expect=201, body={
        "roomId": room["id"], "categoryId": cats[0]["id"], "eventName": f"Лаба 3, проверка {i}",
        "eventDate": day, "startTime": f"{9 + i * 2:02d}:00", "endTime": f"{10 + i * 2:02d}:00",
        "participantCount": 2, "contactPhone": "+79990000000"})
    bookings.append(b["id"])
bid = bookings[0]
call("PUT", f"/api/bookings/{bid}/submit", person=pid, expect=200)
call("POST", f"/api/bookings/{bid}/comments", person=pid, body={"text": "Нужен проектор"}, expect=201)
call("PUT", f"/api/bookings/{bid}/approve", person=ADMIN, body={"reason": "Аудитория свободна"}, expect=200)
call("DELETE", f"/api/bookings/{bookings[2]}", person=pid, expect=204)
org_id = f"lab3-org-{int(time.time())}"
call("POST", "/api/organizations", person=ADMIN, expect=201,
     body={"id": org_id, "name": "Клуб Cassandra", "type": "STUDENT_CLUB", "contact": "@cassandra"})
call("PUT", f"/api/organizations/{org_id}", person=ADMIN, expect=200,
     body={"name": "Клуб Apache Cassandra", "type": "STUDENT_CLUB", "contact": "@cassandra"})
_, got = call("GET", f"/api/bookings/{bid}", person=pid, expect=200)
check("бронь читается из MongoDB, статус APPROVED", got["status"] == "APPROVED")

print("2. Q2: история объекта (events_by_entity)")
hist = eventually(lambda: (lambda p: p if p and len(p.get("items", [])) >= 4 else None)(
    call("GET", f"/api/events/by-entity/BOOKING/{bid}", person=pid, params={"order": "asc"})[1]))
types = [e["eventType"] for e in (hist or {}).get("items", [])]
check("история брони в хронологическом порядке",
      types == ["BOOKING_CREATED", "BOOKING_SUBMITTED", "BOOKING_COMMENT_ADDED", "BOOKING_APPROVED"], str(types))
approved = next((e for e in hist["items"] if e["eventType"] == "BOOKING_APPROVED"), {}) if hist else {}
check("в событии записаны автор и переход статуса",
      approved.get("actorId") == ADMIN and approved.get("payload", {}).get("to") == "APPROVED", str(approved.get("payload")))
desc = call("GET", f"/api/events/by-entity/BOOKING/{bid}", person=pid)[1]
check("order=desc возвращает новые первыми", desc["items"][0]["eventType"] == "BOOKING_APPROVED")
st, _ = call("GET", f"/api/events/by-entity/BOOKING/{bid}", person=call(
    "POST", "/api/persons", body={"lastName": "Чужой", "firstName": "Пётр", "middleName": "-", "role": "STUDENT"})[1]["isuId"])
check("чужую историю брони не отдаём", st == 403, f"status {st}")

print("3. Q1 (обязательный): события типа за период (events_by_type)")
period = {"type": "BOOKING_CREATED", "from": started.isoformat(timespec="seconds"),
          "to": (dt.datetime.now().astimezone() + dt.timedelta(minutes=1)).isoformat(timespec="seconds")}
page = eventually(lambda: (lambda p: p if p and p.get("size", 0) >= 3 else None)(
    call("GET", "/api/events/by-type", person=ADMIN, params={**period, "size": 100})[1]))
mine = [e for e in (page or {}).get("items", []) if e["entityId"] in bookings]
check("все 3 созданные брони найдены за период", len(mine) == 3, f"{len(mine)} из 3")
check("у событий есть бакет и номер в потоке", all(e.get("bucket") is not None and e.get("seq") for e in mine))
times = [e["occurredAt"] for e in page["items"]]
check("сортировка по времени убыванием", times == sorted(times, reverse=True))

seen, token, pages = [], None, 0
while True:
    params = {**period, "size": 1}
    if token:
        params["pageToken"] = token
    p = call("GET", "/api/events/by-type", person=ADMIN, params=params, expect=200)[1]
    seen += [e["eventId"] for e in p["items"]]
    pages += 1
    token = p.get("nextPageToken")
    if not token or pages > 1000:
        break
check("курсорная пагинация size=1 проходит весь период без дублей",
      len(seen) == len(set(seen)) == page["size"], f"{pages} стр., {len(seen)} событий")
empty = call("GET", "/api/events/by-type", person=ADMIN, params={
    "type": "BOOKING_CREATED", "from": "2000-01-01", "to": "2000-01-02"})[1]
check("пустой период -> пустая страница", empty["size"] == 0)
st, _ = call("GET", "/api/events/by-type", person=pid, params={"type": "BOOKING_CREATED"})
check("Q1 доступен только администратору", st == 403, f"status {st}")
one = call("GET", "/api/events/by-type", person=ADMIN, params={**period, "consistency": "ONE"})[1]
check("уровень согласованности задаётся параметром", one["consistency"] == "ONE")

print("4. Q3: действия пользователя (events_by_actor)")
acts = eventually(lambda: (lambda p: p if p and p.get("size", 0) >= 7 else None)(
    call("GET", f"/api/events/by-actor/{pid}", person=pid, params={"from": started.isoformat(timespec="seconds")})[1]))
kinds = sorted({e["eventType"] for e in (acts or {}).get("items", [])})
check("действия пользователя собраны в одном потоке",
      {"PERSON_CREATED", "BOOKING_CREATED", "BOOKING_SUBMITTED", "BOOKING_COMMENT_ADDED",
       "BOOKING_DELETED"} <= set(kinds), str(kinds))
admin_acts = call("GET", f"/api/events/by-actor/{ADMIN}", person=ADMIN,
                  params={"from": started.isoformat(timespec="seconds")})[1]
check("действия администратора: одобрение и организация",
      {"BOOKING_APPROVED", "ORGANIZATION_CREATED", "ORGANIZATION_UPDATED"}
      <= {e["eventType"] for e in admin_acts["items"]})
org = call("GET", f"/api/events/by-entity/ORGANIZATION/{org_id}", person=ADMIN)[1]
upd = next((e for e in org["items"] if e["eventType"] == "ORGANIZATION_UPDATED"), {})
check("изменение организации хранит значения до и после",
      upd.get("payload", {}).get("before", {}).get("name") == "Клуб Cassandra"
      and upd.get("payload", {}).get("after", {}).get("name") == "Клуб Apache Cassandra")

print()
if FAILED:
    print(f"ПРОВАЛЕНО проверок: {len(FAILED)}")
    sys.exit(1)
print("Все проверки пройдены")
