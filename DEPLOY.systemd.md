# Deploy Auger เป็น systemd service บน Ubuntu

ทางเลือกแทน `DEPLOY.ubuntu.md` ที่รันด้วย Docker วิธีนี้ไม่มี container มาคั่น
เหลือแค่ binary ตัวเดียวใน `/usr/local/bin` กับ unit ไฟล์เดียว เหมาะเมื่อเครื่อง
Ubuntu ไม่ได้มี Docker อยู่แล้ว หรือเมื่อ client อยู่คนละเครื่องจึงไม่ได้ประโยชน์
จาก Docker network อยู่ดี

## รู้ไว้ก่อนตัดสินใจ

สองข้อนี้กำหนดว่าคุณจะเปิดพอร์ตแบบไหนได้บ้าง

**auger ไม่มี TLS** ค้นทั้ง `src/server/` ไม่มี TLS ใด ๆ — query ที่ส่งไปและผลลัพธ์
ที่ส่งกลับวิ่งเป็น cleartext ทั้งหมด

**`auth = "scram"` ไม่ได้ทำ SCRAM** ที่ `server/mod.rs` ทั้ง `Md5` และ `Scram`
ถูก map ไปที่ `Md5PasswordAuthStartupHandler` ตัวเดียวกัน จึงเป็น MD5
authentication ซึ่ง PostgreSQL 14 ขึ้นไปเลิกใช้เป็นค่าเริ่มต้นแล้ว ตัวรหัสผ่านเอง
ไม่ได้วิ่งข้ามสายเพราะเป็น challenge-response แต่ก็เท่านั้น

แปลว่า: บน private VLAN ที่คุณควบคุมได้ ระดับนี้พอ ๆ กับที่ Drill port 31010
เป็นอยู่ ยอมรับได้ตามมาตรฐาน internal analytics — แต่ห้ามพาดผ่านอินเทอร์เน็ต
ถ้าต้องข้ามเน็ตเวิร์กที่ไม่ไว้ใจ ให้ทำ SSH tunnel หรือ WireGuard แล้วให้ auger
ฟังแค่ `127.0.0.1`

## สิ่งที่ต้องมี

Ubuntu 22.04 หรือ 24.04, Rust 1.85 ขึ้นไป (โปรเจกต์ใช้ edition 2024)

การ build ใช้ RAM ราว 4 GB และ 10–20 นาทีในครั้งแรก เครื่องที่ RAM น้อยควรเพิ่ม
swap ก่อน ไม่งั้น `cc` จะโดน OOM killer ตอน link

```bash
free -g                              # ดู RAM ที่เหลือ
sudo fallocate -l 4G /swapfile       # ถ้าไม่พอ
sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
```

## 1. ลง Rust แล้ว build

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

git clone https://github.com/peem555643/auger.git
cd auger
cargo build --release --locked
```

`--locked` บังคับให้ใช้เวอร์ชันใน `Cargo.lock` เป๊ะ ๆ ไม่ให้ cargo ไปหยิบ
dependency เวอร์ชันใหม่กว่าที่ยังไม่ผ่านการทดสอบ

ได้ binary ที่ `target/release/auger` ขนาดราว 100 MB

## 2. ติดตั้ง

```bash
sudo ./deploy/install.sh
```

สคริปต์จะ:

- ตรวจว่า binary รันได้บนเครื่องนี้จริง (จับกรณี glibc ไม่ตรงตั้งแต่ตอนนี้ แทนที่จะ
  ไปเจอเป็น exit code เปล่า ๆ ตอน `systemctl start`)
- สร้าง system user `auger` (nologin, ไม่มี home)
- คัดลอก binary ไป `/usr/local/bin/auger`
- วาง `/etc/auger/auger.toml` (0640 root:auger — มีรหัสผ่าน cleartext)
- วาง `/etc/auger/auger.env` (0600 root:root — มี credential ของ Mongo)
- วาง unit แล้ว `enable` ไว้ แต่**ยังไม่ start** เพราะ config ยังเป็น CHANGEME

รันซ้ำได้ปลอดภัย — binary กับ unit จะถูกทับ แต่ config ที่มีอยู่แล้วไม่ถูกแตะ

## 3. ตั้งค่า

### credential ของ Mongo

```bash
sudo nano /etc/auger/auger.env
```

```
AUGER_MONGO_URI=mongodb://auger:รหัสผ่าน@10.199.188.53:27017/?authSource=admin
AUGER_LISTEN=10.0.0.5:5433
AUGER_LOG=info
```

ควรสร้าง Mongo user เฉพาะสำหรับ auger ที่มีสิทธิ์อ่านอย่างเดียว ไม่ใช้ `admin`:

```javascript
use admin
db.createUser({
  user: "auger",
  pwd: passwordPrompt(),
  roles: [ { role: "read", db: "shop" } ]
})
```

ตั้งรหัสผ่านด้วย `openssl rand -hex 24` จะได้ไม่ต้อง percent-encode

`AUGER_LISTEN` ให้ระบุ IP ของ interface ที่ต้องการตรง ๆ ดีกว่าใส่ `0.0.0.0`
แล้วไปหวังพึ่ง firewall เพราะกฎ firewall ถูกแก้ทีหลังโดยคนอื่นได้ แต่ที่ผูกไว้กับ
interface เดียวจะไม่เปลี่ยนตาม

> ไฟล์นี้ systemd อ่านเอง ไม่ใช่ shell — ไม่มีการแตกตัวแปร ไม่มี command
> substitution และห้ามครอบ value ด้วย quote

### database กับรหัสผ่านฝั่ง SQL

```bash
sudo nano /etc/auger/auger.toml
```

```toml
[server]
auth = "md5"

[server.users]
superset = "รหัสผ่านสำหรับ Superset"

[mongo]
databases = ["shop"]
```

struct ของ config ใช้ `deny_unknown_fields` — พิมพ์ชื่อ key ผิดตัวเดียว service
จะไม่ขึ้น ไม่ใช่เงียบ ๆ ข้ามไป

## 4. ทดสอบก่อน start

ขั้นนี้อย่าข้าม มันแยก "ต่อ Mongo ไม่ได้" ออกจาก "มองไม่เห็น collection" ออกจาก
"unit เขียนผิด" ซึ่งจากฝั่ง client เห็นเป็นอาการเดียวกันหมด

```bash
sudo systemd-run --pty --uid=auger --gid=auger \
     -p EnvironmentFile=/etc/auger/auger.env \
     /usr/local/bin/auger --config /etc/auger/auger.toml --describe
```

ใช้ `systemd-run` เพราะมัน parse `EnvironmentFile` ด้วยกฎเดียวกับ unit จริง และรัน
ด้วย user เดียวกัน ถ้าผ่านตรงนี้ สิ่งเดียวที่ยังไม่ได้ทดสอบคือตัว listener

ควรได้ผลประมาณ:

```
schema shop (3 tables)
  orders  (5000 rows)
    _id        Utf8                 bson=objectId
    createdAt  Timestamp(ms,"UTC")  bson=date
```

ถ้าคอลัมน์หาย ให้ขึ้น `sample_size` ใน `auger.toml`

## 5. เปิดใช้งาน

```bash
sudo systemctl start auger
systemctl status auger
journalctl -u auger -f
```

รอเห็น `accepting PostgreSQL connections`

## 6. เปิด firewall เฉพาะเครื่องที่ต้องใช้

ไม่มี TLS และ auth เป็น MD5 กฎนี้จึงทำงานจริง ไม่ใช่แค่พิธีกรรม

```bash
sudo ufw allow from <IP ของเครื่อง Superset> to any port 5433 proto tcp
sudo ufw status numbered
```

ต่างจากกรณี Docker ตรงที่ **ufw ใช้ได้จริงกับ service ที่รันบน host** — ที่ ufw
เอาไม่อยู่คือพอร์ตที่ Docker publish เอง เพราะ Docker เขียนกฎ iptables ของตัวเอง
อยู่เหนือ ufw

## 7. ต่อจาก Superset (คนละเครื่อง)

ทดสอบจากเครื่อง Superset ก่อนว่าถึงกันจริง:

```bash
nc -zv <IP ของ auger> 5433
```

แล้วเพิ่ม connection ใน Superset — **Settings → Database Connections →
+ Database → PostgreSQL**:

```
postgresql+psycopg2://superset:รหัสผ่าน@<IP ของ auger>:5433/shop
```

driver `psycopg2` ติดมากับ image ของ Superset อยู่แล้ว ไม่ต้องลง connector เพิ่ม
เหมือนตอนใช้ Drill

Mongo database หนึ่งตัว = หนึ่ง schema ใน SQL ดังนั้นชื่อตารางเปลี่ยนจาก
`mongo.shop.orders` ของ Drill เหลือ `shop.orders` — dashboard เดิมต้องแก้ตรงนี้

**อย่าติ๊ก `Allow DML`, `Allow CTAS`, `Allow CVAS`** — auger เป็น read-only
Superset จะพยายาม `CREATE TABLE` แล้วโดนปฏิเสธ

## งานประจำวัน

```bash
systemctl status auger
journalctl -u auger -f
journalctl -u auger --since "1 hour ago"
sudo systemctl restart auger
```

schema ที่ infer ได้เก็บอยู่ที่ `/var/lib/auger/catalog.json` ซึ่ง systemd สร้างและ
ถือสิทธิ์ให้ผ่าน `StateDirectory` — ตั้งใจให้อยู่ข้าม restart เพราะการ re-sample
ทุกครั้งแปลว่า type ของคอลัมน์อาจเปลี่ยนใต้ dashboard ที่กำลังใช้งาน

เพิ่ม collection ใหม่แล้วอยากให้เห็นทันที:

```bash
sudo rm /var/lib/auger/catalog.json && sudo systemctl restart auger
```

ไม่มีคำสั่ง refresh แบบไม่ restart — `CALL auger_refresh(...)` ที่คอมเมนต์ใน
`config.rs` พูดถึงยังไม่ได้ถูกเขียนขึ้นจริง

## อัปเดตเวอร์ชันใหม่

```bash
cd auger
git pull
cargo build --release --locked
sudo ./deploy/install.sh      # ทับ binary + unit, ไม่แตะ config
sudo systemctl restart auger
```

## เมื่อไม่ขึ้น

| อาการ | สาเหตุที่พบบ่อย |
|---|---|
| `cc: fatal error: Killed signal terminated` | RAM ไม่พอตอน link — เพิ่ม swap |
| `status=1/FAILURE` ทันทีที่ start | config ผิด — `journalctl -u auger -n 50` จะบอกชื่อ key ที่ไม่รู้จัก |
| `unknown field` ใน log | `deny_unknown_fields` — พิมพ์ชื่อ key ผิด |
| ต่อ Mongo ไม่ได้ | firewall ระหว่างเครื่อง — `nc -zv 10.199.188.53 27017` |
| client ต่อไม่ติดแต่ service ขึ้นปกติ | `AUGER_LISTEN` ผูกกับ `127.0.0.1` หรือ ufw ยังไม่เปิด — `sudo ss -ltnp \| grep 5433` |
| `password authentication failed` | `[server.users]` ยังไม่ได้ตั้ง หรือ `auth` ยังเป็น `trust` |

## ถอนการติดตั้ง

```bash
sudo systemctl disable --now auger
sudo rm /etc/systemd/system/auger.service /usr/local/bin/auger
sudo systemctl daemon-reload
sudo rm -rf /etc/auger /var/lib/auger
sudo userdel auger
```
