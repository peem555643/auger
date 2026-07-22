# Deploy Auger บน Ubuntu (Docker)

รันเป็น container เดียวบนเครื่อง Ubuntu ต่อออกไปหา MongoDB ที่อยู่เซิร์ฟเวอร์อื่น
หรือ Atlas ตัวโค้ดเป็น Rust ล้วนและไม่มีส่วนใดผูกกับ Windows — ที่เพิ่มเข้ามาคือ
วิธี build/รันบน Linux เท่านั้น

## สิ่งที่ต้องมีบนเครื่องปลายทาง

Ubuntu 22.04 หรือ 24.04, Docker Engine พร้อม compose plugin เท่านั้น
ไม่ต้องลง Rust, ไม่ต้องลง MongoDB client

```bash
# ติดตั้ง Docker จาก repo ของ Docker เอง — แพ็กเกจ docker.io ใน Ubuntu
# มักเก่าเกินกว่าจะมี `docker compose` (แบบไม่มีขีด)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"     # ต้อง logout/login ใหม่หนึ่งครั้ง
docker compose version
```

การ build ต้องใช้ RAM ราว 4 GB และเวลา 10–20 นาทีในครั้งแรก (DataFusion + Arrow
เป็นก้อนใหญ่) เครื่อง 2 GB ควรเพิ่ม swap ก่อน ไม่งั้น `cc` จะโดน OOM killer

## ขั้นตอน

คัดลอกโฟลเดอร์โปรเจกต์ไปวางบนเครื่อง Ubuntu (rsync/scp/git clone — ไม่ต้องเอา
`target/` ไป เพราะเป็น artifact ของ Windows ที่ Linux ใช้ไม่ได้ และ `.dockerignore`
กันไว้แล้ว) จากนั้น:

```bash
cd auger

cp .env.example .env
nano .env                 # ใส่ AUGER_MONGO_URI ของ Atlas หรือ replica set

cp auger.prod.toml auger.toml
nano auger.toml           # เลือก databases ที่จะเปิด, ตั้ง auth ถ้าจำเป็น

docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml logs -f
```

### ตรวจว่าต่อ Mongo ติดจริงก่อนเปิดใช้งาน

`--describe` จะพิมพ์ catalog ที่ค้นเจอพร้อม column ที่ infer ได้ แล้วจบการทำงาน
คุ้มมากที่จะรันก่อน เพราะมันแยก "ต่อ Mongo ไม่ได้" ออกจาก "มองไม่เห็น collection"
และ "SQL เขียนผิด" ซึ่งจากฝั่ง client เห็นเป็นอาการเดียวกันหมด

```bash
docker compose -f docker-compose.prod.yml run --rm auger \
    --config /etc/auger/auger.toml --describe
```

> `docker run`/`compose run` ที่ใส่ flag เอง จะไปแทนที่ `CMD` ทั้งชุด จึงต้องพิมพ์
> `--config /etc/auger/auger.toml` ซ้ำด้วยทุกครั้ง

### ต่อจาก client

พอร์ตถูกผูกไว้ที่ `127.0.0.1:5433` เท่านั้น (เหตุผลอยู่หัวข้อความปลอดภัยด้านล่าง)
บนเครื่องนั้นเองต่อได้ตรง ๆ:

```bash
psql "postgresql://auger@localhost:5433/shop"
```

จากเครื่องอื่น ให้ทำ SSH tunnel:

```bash
ssh -N -L 5433:localhost:5433 user@ubuntu-host
# แล้วชี้ DBeaver / Power BI / Metabase ไปที่ localhost:5433
```

## ต่อเข้า Superset

Superset คุยกับ auger ผ่าน driver `postgresql+psycopg2` ที่ติดมากับ image อยู่แล้ว
ไม่ต้องลง connector เพิ่มเหมือนตอนใช้ Drill

ถ้า Superset รันใน Docker บนเครื่องเดียวกัน มันจะ **ต่อ `127.0.0.1:5433` ไม่ถึง**
เพราะ loopback ที่เห็นจากใน container คือของ container เอง ไม่ใช่ของ host
`docker-compose.superset.yml` แก้จุดนี้ด้วยการเอา auger ไปแปะบน network ของ
Superset:

```bash
docker network ls                                   # หาชื่อ network ของ Superset
echo 'SUPERSET_NETWORK=superset_default' >> .env    # ใส่ชื่อที่เจอจริง

docker compose -f docker-compose.prod.yml -f docker-compose.superset.yml up -d
```

ยืนยันว่าเห็นกันจริงก่อนไปตั้งค่าใน UI — ถ้าข้ามขั้นนี้ อาการ "ต่อ network ไม่ได้"
กับ "URI พิมพ์ผิด" จะแยกไม่ออก:

```bash
docker compose exec superset_app \
    python -c "import socket; socket.create_connection(('auger',5433),5); print('ok')"
```

จากนั้นใน Superset ไปที่ **Settings → Database Connections → + Database →
PostgreSQL** แล้วใส่:

```
postgresql+psycopg2://auger@auger:5433/<ชื่อ database ใน mongo>
```

ค่า database ท้าย URI แค่กำหนด schema เริ่มต้น — Mongo database ตัวอื่นที่เปิดไว้
ใน `auger.toml` ยังมองเห็นเป็น schema อื่นใน SQL Lab อยู่ดี ชื่อตารางจึงเปลี่ยนจาก
`mongo.shop.orders` ของ Drill เหลือ `shop.orders`

**อย่าติ๊ก `Allow DML`, `Allow CTAS`, `Allow CVAS`** — auger เป็น read-only
Superset จะพยายาม `CREATE TABLE` แล้วโดนปฏิเสธ

ถ้า Superset ไม่ได้อยู่ใน Docker (ลงตรงบน host หรืออยู่คนละเครื่อง) ไม่ต้องใช้
overlay นี้ — ดูหัวข้อความปลอดภัยด้านล่างเรื่องการเปิดพอร์ต

## ความปลอดภัย

ค่า `auth = "trust"` ที่มาเป็นค่าเริ่มต้น หมายถึงรับทุก connection โดยไม่ถามรหัสผ่าน
มันปลอดภัยได้เฉพาะตอนที่พอร์ตยังผูกกับ loopback — ซึ่งเป็นเหตุผลที่
`docker-compose.prod.yml` เขียน `"127.0.0.1:5433:5433"` ไว้ ไม่ใช่ `"5433:5433"`

ถ้าต้องเปิดให้เครื่องอื่นต่อตรงจริง ๆ ให้ทำสองอย่างพร้อมกัน:

1. ใน `auger.toml` เปลี่ยนเป็น `auth = "scram"` และเติม `[server.users]`
2. ค่อยแก้ compose เป็น `"5433:5433"` แล้วเปิด firewall เฉพาะ IP ต้นทางที่ต้องการ
   (`sudo ufw allow from 10.0.0.0/24 to any port 5433 proto tcp`)

จำไว้ว่า Docker เขียนกฎ iptables ของตัวเองที่ **อยู่เหนือ ufw** — พอร์ตที่ publish
แบบ `"5433:5433"` จะทะลุ `ufw deny` ออกไปได้ การผูกกับ `127.0.0.1` คือวิธีที่
เชื่อถือได้กว่าการหวังพึ่ง ufw อย่างเดียว

อีกข้อ: auger เป็น read-only — `INSERT`/`UPDATE`/`DELETE` ถูกปฏิเสธในตัวมันเอง
แต่ก็ควรใช้ Mongo user ที่มีสิทธิ์ `read` เท่านั้นในการ URI อยู่ดี

## งานประจำวัน

```bash
cd auger
alias dc='docker compose -f docker-compose.prod.yml'

dc ps                         # สถานะ + healthcheck
dc logs -f --tail=100
dc restart
dc up -d --build              # deploy โค้ดใหม่
dc down                       # หยุด (catalog cache ยังอยู่)
dc down -v                    # หยุด + ลบ cache schema ที่ infer ไว้
```

schema ที่ infer ได้จะถูกเก็บใน named volume `auger_auger-catalog` ที่
`/var/lib/auger/catalog.json` ตั้งใจให้เป็นแบบนั้น เพราะการ re-sample ทุกครั้งที่
restart แปลว่า type ของคอลัมน์อาจเปลี่ยนใต้ dashboard ที่กำลังใช้งานอยู่ ถ้าเพิ่ม
collection ใหม่แล้วอยากให้เห็นทันที ให้ `dc down -v && dc up -d`

## เมื่อ build ไม่ผ่าน

| อาการ | สาเหตุที่พบบ่อย |
|---|---|
| `cc: fatal error: Killed signal terminated` | RAM ไม่พอตอน link — เพิ่ม swap หรือ build บนเครื่องใหญ่กว่าแล้ว push image |
| `failed to select a version ... requires rustc 1.85` | โปรเจกต์ใช้ edition 2024 — อย่าลด `RUST_VERSION` ใน Dockerfile ต่ำกว่า 1.85 |
| build นานทุกครั้งที่แก้โค้ด | ปกติแล้ว layer ของ dependency จะถูก cache ไว้ จะเสียใหม่ก็ต่อเมื่อ `Cargo.toml`/`Cargo.lock` เปลี่ยน |
| `no configuration file found` ตอน start | ลืม `cp auger.prod.toml auger.toml` — compose mount ไฟล์นั้นเข้าไป |

## ถ้าอยาก build ที่เดียวแล้วส่งขึ้นหลายเครื่อง

เครื่องปลายทางไม่ต้อง build เองก็ได้ ถ้ามี registry ก็ push ตามปกติ ถ้าไม่มี
ส่งเป็นไฟล์ tar ได้เลย:

```bash
docker build -t auger:0.1.0 .
docker save auger:0.1.0 | gzip > auger-0.1.0.tar.gz
scp auger-0.1.0.tar.gz user@target:/tmp/

# บนเครื่องปลายทาง
gunzip -c /tmp/auger-0.1.0.tar.gz | docker load
docker compose -f docker-compose.prod.yml up -d      # ไม่ต้องใส่ --build
```

ข้อควรระวัง: image ผูกกับสถาปัตยกรรม CPU — build บน x86_64 แล้วเอาไปรันบน ARM
(Graviton, Ampere) ไม่ได้ ต้อง build บนเครื่องที่ arch ตรงกัน

## พัฒนาต่อบน Ubuntu

`docker-compose.yml` (ตัว dev เดิม) ใช้ได้บน Linux เหมือนกัน — มันขึ้น MongoDB
สำหรับทดสอบพร้อม container ที่มี Rust toolchain ส่วน `x.sh` คือ `x.ps1` ฉบับ bash:

```bash
chmod +x x.sh
./x.sh test
./x.sh run -- --mongo-uri mongodb://mongo:27017 --listen 0.0.0.0:5433
```
