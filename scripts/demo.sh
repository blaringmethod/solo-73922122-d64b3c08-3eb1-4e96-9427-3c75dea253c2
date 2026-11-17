#!/usr/bin/env bash
# End-to-end demonstration of the delivery guarantees.
#
#   docker compose up -d --build       # from the repository root
#   ./scripts/demo.sh
#
# The script only uses curl + python3 (for JSON pretty output) and talks to
# the three containers.  It is idempotent-ish: re-running it just adds more
# demo mail, which is also a useful way to watch duplicate-report protection.
set -euo pipefail

APP_A="${APP_A:-http://localhost:8000}"
APP_B="${APP_B:-http://localhost:8001}"
DEMO="${DEMO:-http://localhost:8081}"

step() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
json() { python3 -m json.tool; }

wait_for() {
  local url="$1" name="$2" tries="${3:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timeout waiting for $name ($url)" >&2
  return 1
}

step "等待服务就绪"
wait_for "$DEMO/healthz" "demo-imap" 120
wait_for "$APP_A/healthz" "web-a" 120
wait_for "$APP_B/healthz" "web-b" 120

deliver() { curl -fsS -X POST "$DEMO/ctl/deliver" -H 'Content-Type: application/json' -d "$1"; echo; }

step "1. 投递一封含两个附件的邮件，由 web-a 采集"
deliver '{"subject":"演示-双附件","attachments":[
  {"filename":"DEMO-R1.csv","report_no":"DEMO-R1","sample_no":"S001","item":"ALT","value":"21"},
  {"filename":"DEMO-R2.csv","report_no":"DEMO-R2","sample_no":"S002","item":"GLU","value":"5.4"}]}'
curl -fsS -X POST "$APP_A/api/scan" | json

step "2. 人工已读（模拟有人在邮箱里先打开）仍会被采集"
deliver '{"subject":"演示-人工已读","attachments":[
  {"filename":"DEMO-R3.csv","report_no":"DEMO-R3","sample_no":"S003","item":"K","value":"4.1"}]}'
# 演示源没有单独的“设为已读”接口，这里直接用 IMAP 客户端改标记。
python3 - "$DEMO" "${IMAP_HOST:-localhost}" "${IMAP_PORT:-1143}" <<'PY'
import imaplib, sys, json, urllib.request
host, port = sys.argv[2], int(sys.argv[3])
try:
    cfg = json.load(urllib.request.urlopen(sys.argv[1] + "/api/state"))
    uids = [m["uid"] for m in cfg["messages"] if m["subject"] == "演示-人工已读"]
except Exception as exc:  # noqa: BLE001
    print("skip pre-read step:", exc)
    raise SystemExit(0)
c = imaplib.IMAP4(host, port)
c.login("lab", "lab-pass")
c.select("INBOX")
for uid in uids:
    c.uid("STORE", str(uid), "+FLAGS.SILENT", "(\\Seen)")
c.logout()
print("marked pre-read message(s):", uids)
PY
curl -fsS -X POST "$APP_B/api/scan" | json

step "3. 同号同原件复用：新邮件带同样的 DEMO-R1 附件"
deliver '{"subject":"演示-复用原件","attachments":[
  {"filename":"DEMO-R1.csv","report_no":"DEMO-R1","sample_no":"S001","item":"ALT","value":"21"}]}'
curl -fsS -X POST "$APP_A/api/scan" | json

step "4. 同号异文整封拒收（DEMO-R2 内容不同），且不会带入其中的新报告号"
deliver '{"subject":"演示-同号异文","attachments":[
  {"filename":"DEMO-R9.csv","report_no":"DEMO-R9","sample_no":"S009","item":"NEW","value":"1"},
  {"filename":"DEMO-R2-bad.csv","content":"报告号,样本号,项目,测值\nDEMO-R2,S999,OTHER,999\n"}]}'
curl -fsS -X POST "$APP_A/api/scan" | json
curl -fsS "$APP_A/api/messages?mark_state=marked" \
  | python3 -c 'import json,sys
rows=json.load(sys.stdin)
bad=[r for r in rows if r["decision"]=="rejected"]
print(json.dumps(bad[:2], ensure_ascii=False, indent=2))
assert not any("DEMO-R9" in (r.get("report_nos") or "") for r in rows), "R9 leaked"'

step "5. 标记后断连：STORE 成功但回执丢失，本地保持待标记"
curl -fsS -X POST "$DEMO/ctl/drop-store" -H 'Content-Type: application/json' -d '{"enabled":true}' >/dev/null
deliver '{"subject":"演示-标记后断连","attachments":[
  {"filename":"DEMO-R4.csv","report_no":"DEMO-R4","sample_no":"S004","item":"NA","value":"139"}]}'
curl -fsS -X POST "$APP_A/api/scan" | json
curl -fsS "$APP_A/api/status" | python3 -c 'import json,sys;print("pending:",json.load(sys.stdin)["stats"]["pending_marks"])'
step "5b. 页面/接口重试补标记"
curl -fsS -X POST "$APP_A/api/retry-marks" | json
curl -fsS "$APP_A/api/status" | python3 -c 'import json,sys
s=json.load(sys.stdin)["stats"]; print("pending:",s["pending_marks"],"marked:",s["marked"])'

step "6. 两实例并发：同一时刻只允许一个推进邮箱"
deliver '{"subject":"演示-并发","attachments":[
  {"filename":"DEMO-R5.csv","report_no":"DEMO-R5","sample_no":"S005","item":"BUN","value":"6.2"}]}'
(curl -fsS -o /tmp/scan-a.json -w "web-a: %{http_code}\n" -X POST "$APP_A/api/scan" &
 curl -fsS -o /tmp/scan-b.json -w "web-b: %{http_code}\n" -X POST "$APP_B/api/scan" &
 wait) || true
cat /tmp/scan-a.json 2>/dev/null | json || true
cat /tmp/scan-b.json 2>/dev/null | json || true

step "7. 邮箱重建：UIDVALIDITY 变化 + UID 1 被新邮件重用"
# 7a. 先制造一条“标记后断连”的待标记记录，它属于旧 UIDVALIDITY。
curl -fsS -X POST "$DEMO/ctl/drop-store" -H 'Content-Type: application/json' -d '{"enabled":true}' >/dev/null
deliver '{"subject":"演示-重建前待标记","attachments":[
  {"filename":"DEMO-R7.csv","report_no":"DEMO-R7","sample_no":"S007","item":"AST","value":"33"}]}'
curl -fsS -X POST "$APP_B/api/scan" | json
BEFORE_UIDVALIDITY=$(curl -fsS "$APP_B/api/status" | python3 -c 'import json,sys;print(json.load(sys.stdin)["mailbox_state"]["uidvalidity"])')
BEFORE_RAW=$(curl -fsS "$APP_A/api/messages?mark_state=pending" | python3 -c 'import json,sys;rows=json.load(sys.stdin);print(rows[0]["raw_sha256"] if rows else "none")')
echo "旧代 uidvalidity=$BEFORE_UIDVALIDITY，待标记原文=${BEFORE_RAW:0:16}…"

# 7b. 清空重建邮箱（UIDVALIDITY+1，UID 从 1 重新分配），再投递全新邮件。
curl -fsS -X POST "$DEMO/ctl/rebuild" -H 'Content-Type: application/json' -d '{"purge":true}' | json
deliver '{"subject":"演示-重建后新邮件-UID1","attachments":[
  {"filename":"DEMO-R6.csv","report_no":"DEMO-R6","sample_no":"S006","item":"AST","value":"44"}]}'
curl -fsS -X POST "$APP_B/api/scan" | json
curl -fsS "$APP_B/api/status" | python3 -c 'import json,sys
d=json.load(sys.stdin); print(json.dumps({"mailbox_state":d["mailbox_state"],"stats":d["stats"]}, ensure_ascii=False, indent=2))'

# 7c. 断言：旧待标记记录变成“旧代失效”，新 UID 1 是另一封邮件，且两代互不串用。
curl -fsS "$APP_A/api/messages?mark_state=stale" | python3 -c "import json,sys
rows=json.load(sys.stdin)
quarantined=[r for r in rows if r['raw_sha256']=='$BEFORE_RAW']
assert quarantined, 'expected the old pending mark to be quarantined as stale'
assert quarantined[0]['uidvalidity']==int('$BEFORE_UIDVALIDITY'), 'quarantined row must belong to old UIDVALIDITY'
print('旧代 pending -> stale:', [(r['uidvalidity'], r['uid'], r['raw_sha256'][:12]) for r in quarantined])"
curl -fsS "$APP_A/api/messages" | python3 -c "import json,sys
rows=json.load(sys.stdin)
new=[r for r in rows if r['uidvalidity']!=int('$BEFORE_UIDVALIDITY') and r['uid']==1]
assert new and new[0]['decision']=='accepted' and 'DEMO-R6' in new[0]['report_nos'], new
assert new[0]['raw_sha256'] != '$BEFORE_RAW', 'UID 1 must not inherit the old receipt'
print('新代 UID 1:', new[0]['decision'], new[0]['mark_state'], new[0]['report_nos'], new[0]['raw_sha256'][:12])"

step "8. 最终入库报告与标记状态"
curl -fsS "$APP_A/api/messages?mark_state=marked" | python3 -c 'import json,sys
rows=json.load(sys.stdin)
for r in sorted(rows, key=lambda x: (x["uidvalidity"], x["uid"]))[-10:]:
    print(r["uidvalidity"], r["uid"], r["decision"], r["mark_state"], r["report_nos"], r["reason"] or "")'

# ---------------------------------------------------------------------------
# 9. 齐套核对：上传清单 → 核对 → 缺项 → 齐套后异值到达
# ---------------------------------------------------------------------------
step "9. 齐套核对（清单上传 → 核对 → 结论）"
# 批号与样本号带时间戳，脚本可重复运行；已受理清单永远不可改。
STAMP=$(date +%s)
BATCH_OK="DEMO-OK-$STAMP"
BATCH_MISS="DEMO-MISS-$STAMP"
SAMPLE_A="Q$STAMP-A"
SAMPLE_B="Q$STAMP-B"
# 先入仓两对“历史”数据，再由清单核对它们。
deliver "{\"subject\":\"演示-齐套历史\",\"attachments\":[
  {\"filename\":\"DEMO-Q1.csv\",\"report_no\":\"DEMO-Q$STAMP-1\",\"sample_no\":\"$SAMPLE_A\",\"item\":\"ALT\",\"value\":\"21\"},
  {\"filename\":\"DEMO-Q2.csv\",\"report_no\":\"DEMO-Q$STAMP-2\",\"sample_no\":\"$SAMPLE_B\",\"item\":\"GLU\",\"value\":\"5.4\"}]}" >/dev/null
curl -fsS -X POST "$APP_A/api/scan" >/dev/null
printf '样本号,项目\n%s,ALT\n%s,GLU\n' "$SAMPLE_A" "$SAMPLE_B" > /tmp/checklist-demo.csv
CREATED=$(curl -fsS -X POST "$APP_A/api/checklists" -F batch_no="$BATCH_OK" \
  -F file=@/tmp/checklist-demo.csv)
echo "$CREATED" | python3 -m json.tool
OK_ID=$(echo "$CREATED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS -X POST "$APP_A/api/checklists/backfill" | python3 -c 'import json,sys
d=json.load(sys.stdin); print("补建:", d["backfill"]["status"], d["backfill"]["reports_projected"], "/", d["backfill"]["report_total"])'
curl -fsS "$APP_B/api/checklists/$OK_ID" | python3 -c 'import json,sys
d=json.load(sys.stdin); print("清单:", d["batch_no"], d["state"], "ready=", d["ready"], d["summary"])
assert d["state"] == "ready" and d["ready"], d'

step "9b. 缺项可见（清单含不存在的样本）"
printf '样本号,项目\nMISSING-%s,XXX\n' "$STAMP" > /tmp/checklist-missing.csv
MISS_ID=$(curl -fsS -X POST "$APP_A/api/checklists" -F batch_no="$BATCH_MISS" \
  -F file=@/tmp/checklist-missing.csv | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "$APP_A/api/checklists/$MISS_ID" | python3 -c 'import json,sys
d=json.load(sys.stdin); print("清单:", d["batch_no"], d["state"], d["summary"])
assert d["pairs"][0]["status"] == "missing" and not d["ready"]'

step "9c. 齐套后收到相关异值 → 立即转为冲突，不再显示可交分析"
curl -fsS -X POST "$DEMO/ctl/deliver" -H 'Content-Type: application/json' \
  -d "{\"subject\":\"演示-齐套后异值\",\"attachments\":[
       {\"filename\":\"DEMO-Q1-alt.csv\",\"report_no\":\"DEMO-Q$STAMP-1B\",
        \"sample_no\":\"$SAMPLE_A\",\"item\":\"ALT\",\"value\":\"99\"}]}" >/dev/null
curl -fsS -X POST "$APP_B/api/scan" | json
curl -fsS "$APP_A/api/checklists/$OK_ID" | python3 -c 'import json,sys
d=json.load(sys.stdin)
print("清单:", d["batch_no"], d["state"], "ready=", d["ready"], d["summary"])
assert d["ready"] is False and any(p["status"]=="conflict" for p in d["pairs"])
print("冲突项:", [(p["sample_no"], [(g["value"], g["report_nos"]) for g in p["values"]])
                  for p in d["pairs"] if p["status"]=="conflict"])'
curl -fsS "$APP_A/checklists/$OK_ID" | grep -q "不可交分析" && echo "页面：不可交分析"
if curl -fsS "$APP_A/checklists/$OK_ID" | grep -q "可交分析" \
   && ! curl -fsS "$APP_A/checklists/$OK_ID" | grep -q "不可交分析"; then
  echo "错误：冲突清单仍显示可交分析" >&2; exit 1
fi

step "9d. 拒收邮件不参与核对"
curl -fsS -X POST "$DEMO/ctl/deliver" -H 'Content-Type: application/json' \
  -d '{"subject":"演示-拒收不参与","attachments":[
       {"filename":"bad.csv","content":"bad,header\n1,2\n"}]}' >/dev/null
curl -fsS -X POST "$APP_A/api/scan" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("拒收:", d["rejected"])'

step "9e. 双实例不再重复累计"
curl -fsS "$APP_A/api/checklists/$OK_ID" > /tmp/list-a.json
curl -fsS "$APP_B/api/checklists/$OK_ID" > /tmp/list-b.json
python3 - <<'PY'
import json
a = json.load(open("/tmp/list-a.json")); b = json.load(open("/tmp/list-b.json"))
assert a["summary"] == b["summary"] and a["state"] == b["state"], (a["summary"], b["summary"])
print("两实例结论一致:", a["summary"])
PY

echo
echo "全部演示步骤完成。浏览器打开 $APP_A 或 $APP_B 查看页面；齐套核对：$APP_A/checklists"
