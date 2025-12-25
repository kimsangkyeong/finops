# -----------------------------------------------------------------
# File Name : finops_lambda_function.py
# Description : AWS Event Bridge로 Scheduling되어 Instance 자동 start/stop/scale resize 처리
# Information : 
#                   1. FinOps 대상 리소스 및 동작 방식
#                       - EC2 :  Instance start / stop
#                       - RDS :  Cluster start / stop
#                       - EKS :  Node resize ( min ~ max )
#                   2. FinOps 처리 대상 및 기준
#                       - 대상 리소스의 Tag에 "finops-start", "finops-stop" key를 포함하는 경우 대상이 됨.
#                       - value는 다음의 포맷으로 등록한다.
#                             type=weekday@description=skip-예외/weekday-주중/everyday-매일
#                              =>  @ 문자의 앞 문자열 : 실행 판단기준
#                                     @ 문자의 뒷 문자열 : 선택 가능 유형과 의미 설명
#                                               .skip : FinOps 처리 대상으로 판단되지만, 처리하지 않음(임시제외목적)
#                                               .weekday : 월~금 주중에만 FinOps 처리
#                                               .everyday : 월~일 매일 FinOps 처리
# =================================================================
# version     date       author      reason
# -----------------------------------------------------------------
#  1.0     2025.10.24    k.s.k       first edit with open.ai
#  1.1     2025.12.25    k.s.k       Information 추가
# -----------------------------------------------------------------
# lambda_function.py
import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import urllib.request
import urllib.error

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# ---------- Environment ----------
ACTION = os.getenv("ACTION", "start").lower()            # 'start' or 'stop'
REGIONS = [r.strip() for r in os.getenv("REGIONS", "ap-northeast-2").split(",") if r.strip()]
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

EKS_DEFAULT_START_SIZE = int(os.getenv("EKS_DEFAULT_START_SIZE", "2"))
EKS_DEFAULT_STOP_SIZE  = int(os.getenv("EKS_DEFAULT_STOP_SIZE", "0"))

HTTP_TIMEOUT = 10
BOTO_CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})
KST = timezone(timedelta(hours=9))  # Asia/Seoul

# ---------- Common utils ----------
def now_kst_str():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

def is_weekday_kst() -> bool:
    """Return True if now (KST) is Mon–Fri."""
    return datetime.now(KST).weekday() < 5  # Mon=0 ... Sun=6

def get_tag_value(tags, key):
    """
    tags can be:
      - EC2: [{'Key': 'Name', 'Value': 'x'}]
      - EKS: {'some': 'value'}
      - RDS ListTagsForResource: [{'Key': 'k', 'Value': 'v'}]
    """
    if tags is None:
        return None
    if isinstance(tags, list):
        for t in tags:
            if t.get("Key") == key:
                return t.get("Value")
    elif isinstance(tags, dict):
        return tags.get(key)
    return None

def get_name_tag_value(tags):
    v = get_tag_value(tags, "Name")
    return v if v else "(no Name tag)"

# --- FinOps parsing: "type=...@description=..." or plain "everyday/weekday/skip"
ALLOWED_TYPES = {"everyday", "skip", "weekday"}

def parse_finops_value(val: str):
    """
    Parse 'type=weekday@description=...'
    Returns (type_str, description, valid)
    Fallback: if value is just 'everyday' or 'weekday' or 'skip', accept it.
    """
    if not val:
        return None, "", False
    if "type=" in val:
        try:
            parts = val.split("@")
            kv = {}
            for p in parts:
                if "=" in p:
                    k, v = p.split("=", 1)
                    kv[k.strip().lower()] = v.strip()
            typ = (kv.get("type") or "").lower()
            desc = kv.get("description", "")
            valid = typ in ALLOWED_TYPES
            return typ, desc, valid
        except Exception:
            return None, "", False
    # fallback: raw value as type (backward compatible)
    val_low = val.lower()
    return (val_low, "", val_low in ALLOWED_TYPES)

def get_finops_info(tags, action: str):
    """
    Return (has_finops_tag, finops_key, finops_raw, finops_type, description, valid)
    """
    finops_key = "finops-start" if action == "start" else "finops-stop"
    raw = get_tag_value(tags, finops_key)
    if not raw:
        return False, finops_key, None, None, "", False
    typ, desc, valid = parse_finops_value(raw)
    return True, finops_key, raw, typ, desc, valid

# ---------- Slack helpers ----------
def slack_post_message(blocks):
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
        logger.warning("Slack env not configured; skip Slack notification.")
        return
    url = "https://slack.com/api/chat.postMessage"
    data = {"channel": SLACK_CHANNEL, "blocks": blocks, "text": "FinOps Auto Start/Stop Report"}
    req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"))
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Authorization", f"Bearer {SLACK_BOT_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            logger.debug(f"Slack response: {resp.read().decode('utf-8', 'ignore')}")
    except urllib.error.HTTPError as e:
        logger.error(f"Slack HTTP error: {e.code} {e.read().decode('utf-8', 'ignore')}")
    except Exception as e:
        logger.exception(f"Slack send failed: {e}")

def _to_str(x):
    try:
        if x is None:
            return ""
        if isinstance(x, str):
            return x
        return str(x)
    except Exception:
        return ""

def md_table(rows):
    """
    Build a monospace table as mrkdwn code block.
    - Coerces all cells to strings safely (prevents TypeError on None).
    """
    if not rows:
        return "```\n(no items)\n```"
    norm = [[_to_str(c) for c in r] for r in rows]
    col_count = max(len(r) for r in norm)
    widths = [0] * col_count
    for r in norm:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    lines = []
    for idx, r in enumerate(norm):
        padded = [(r[i] if i < len(r) else "").ljust(widths[i]) for i in range(col_count)]
        lines.append(" | ".join(padded))
        if idx == 0:
            lines.append(" | ".join(["-" * widths[i] for i in range(col_count)]))
    return "```\n" + "\n".join(lines) + "\n```"

MAX_MSG_BYTES = 3000

def _utf8_len(s: str) -> int:
    try:
        return len((s or "").encode("utf-8"))
    except Exception:
        return 0

def _blk_header(text_plain: str):
    return {"type": "header", "text": {"type": "plain_text", "text": text_plain}}

def _blk_section_md(md_text: str):
    return {"type": "section", "text": {"type": "mrkdwn", "text": md_text}}

def _blk_divider():
    return {"type": "divider"}

def paginate_rows(rows, title):
    """
    Split a table (rows) into multiple Slack messages staying under ~3000 bytes.
    """
    if not rows:
        return [[_blk_header(title), _blk_section_md(md_table([["(empty)"]]))]]

    header = rows[0]
    body = rows[1:]

    current_rows = [header]
    current_blocks = [_blk_header(title), _blk_section_md(md_table(current_rows))]
    messages = []

    def blocks_bytes(blks):
        total = 0
        for b in blks:
            if b.get("type") == "section":
                total += _utf8_len(b.get("text", {}).get("text", ""))
            elif b.get("type") == "header":
                total += _utf8_len(b.get("text", {}).get("text", ""))
        return total

    for r in body:
        cand_rows = current_rows + [r]
        cand_blocks = [_blk_header(title), _blk_section_md(md_table(cand_rows))]
        if blocks_bytes(cand_blocks) <= MAX_MSG_BYTES:
            current_rows = cand_rows
            current_blocks = cand_blocks
        else:
            messages.append(current_blocks)
            current_rows = [header, r]
            current_blocks = [_blk_header(title), _blk_section_md(md_table(current_rows))]

    if current_blocks:
        messages.append(current_blocks)

    if len(messages) > 1:
        total = len(messages)
        for i, blks in enumerate(messages, start=1):
            if blks and blks[0].get("type") == "header":
                orig = blks[0]["text"]["text"]
                blks[0]["text"]["text"] = f"{orig}  [{i}/{total}]"
    return messages

def build_slack_messages(region_results):
    messages = [[
        {"type": "section", "text": {"type": "mrkdwn",
         "text": f":money_with_wings: *FinOps Auto {ACTION.upper()} Report* _(KST {now_kst_str()})_"}},
        _blk_divider()
    ]]

    for region, res in region_results.items():
        with_rows = [["Lambda 실행시간","리소스 유형","Tag Name","리소스 id","리소스 endpoint",
                      "finops-* value","실행결과","오류"]]
        with_rows.extend(res.get("with", []))

        without_rows = [["Lambda 실행시간","리소스유형","Tag Name","리소스 id","리소스 endpoint"]]
        without_rows.extend(res.get("without", []))

        messages += paginate_rows(with_rows, f"[{region}] 대상 리소스(태그 있음)")
        messages += paginate_rows(without_rows, f"[{region}] 비대상 리소스(태그 없음)")

    return messages

def send_chunks(messages):
    for blks in messages:
        slack_post_message(blks)

# ---------- Core action decision ----------
def should_execute(finops_type: str) -> (bool, str):
    """
    Decide whether to execute based on finops_type (no description gating).
    Returns (do_execute, reason)
    - 'skip'     -> (False, 'skip ok')
    - 'weekday'  -> execute only Mon–Fri (KST); otherwise 'skip ok (not weekday)'
    - 'everyday' -> always execute (no gating)
    """
    if finops_type == "skip":
        return False, "skip ok"
    if finops_type == "weekday":
        if is_weekday_kst():
            return True, ""
        else:
            return False, "skip ok (not weekday)"
    # everyday -> always execute
    return True, ""

# ---------- EC2 ----------
def process_ec2(region):
    ec2 = boto3.client("ec2", region_name=region, config=BOTO_CONFIG)
    with_tag, without_tag = [], []

    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate():
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                iid = inst["InstanceId"]
                tags = inst.get("Tags", [])
                name_tag = get_name_tag_value(tags)

                has_finops, finops_key, finops_raw, finops_type, finops_desc, finops_valid = get_finops_info(tags, ACTION)
                endpoint = inst.get("PublicDnsName") or inst.get("PrivateIpAddress") or ""
                result, err = "-", ""

                if has_finops:
                    try:
                        state = (inst.get("State") or {}).get("Name")
                        logger.debug(f"[EC2] {iid} state={state}, action={ACTION}, finops_type={finops_type}, valid={finops_valid}")

                        if not finops_valid:
                            result = "value setting error"
                        else:
                            do_exec, reason = should_execute(finops_type)
                            if not do_exec:
                                result = reason
                            else:
                                if ACTION == "start" and state in ("stopped", "stopping"):
                                    ec2.start_instances(InstanceIds=[iid])
                                    result = "start: requested"
                                elif ACTION == "stop" and state in ("running", "pending"):
                                    ec2.stop_instances(InstanceIds=[iid])
                                    result = "stop: requested"
                                else:
                                    result = f"no-op (state={state})"
                    except ClientError as e:
                        err = str(e)
                        logger.exception(f"[EC2] action failed {iid}")

                    with_tag.append([
                        now_kst_str(), "EC2", name_tag, iid, endpoint, finops_raw or "", result, err
                    ])
                else:
                    without_tag.append([
                        now_kst_str(), "EC2", name_tag, iid, endpoint
                    ])
    return with_tag, without_tag

# ---------- RDS / Aurora ----------
def list_rds_tags(rds, arn):
    try:
        return rds.list_tags_for_resource(ResourceName=arn).get("TagList", [])
    except ClientError:
        logger.exception(f"ListTagsForResource failed: {arn}")
        return []

def process_rds(region):
    rds = boto3.client("rds", region_name=region, config=BOTO_CONFIG)
    with_tag, without_tag = [], []

    # DB Instances
    insts = rds.describe_db_instances().get("DBInstances", [])
    for inst in insts:
        arn = inst["DBInstanceArn"]
        tags = list_rds_tags(rds, arn)
        name_tag = get_name_tag_value(tags)

        has_finops, finops_key, finops_raw, finops_type, finops_desc, finops_valid = get_finops_info(tags, ACTION)
        endpoint = (inst.get("Endpoint") or {}).get("Address", "")
        dbid = inst["DBInstanceIdentifier"]

        result, err = "-", ""
        if has_finops:
            try:
                status = inst.get("DBInstanceStatus")
                logger.debug(f"[RDS-Instance] {dbid} status={status} action={ACTION} finops_type={finops_type} valid={finops_valid}")

                if not finops_valid:
                    result = "value setting error"
                else:
                    do_exec, reason = should_execute(finops_type)
                    if not do_exec:
                        result = reason
                    else:
                        if ACTION == "start" and status in ("stopped", "stopping"):
                            rds.start_db_instance(DBInstanceIdentifier=dbid)
                            result = "start: requested"
                        elif ACTION == "stop" and status in ("available", "backing-up", "modifying"):
                            rds.stop_db_instance(DBInstanceIdentifier=dbid)
                            result = "stop: requested"
                        else:
                            result = f"no-op (status={status})"
            except ClientError as e:
                err = str(e)
                logger.exception(f"[RDS-Instance] action failed {dbid}")

            with_tag.append([
                now_kst_str(), "RDS-Instance", name_tag, dbid, endpoint, finops_raw or "", result, err
            ])
        else:
            without_tag.append([
                now_kst_str(), "RDS-Instance", name_tag, dbid, endpoint
            ])

    # Aurora Clusters
    clusters = rds.describe_db_clusters().get("DBClusters", [])
    for cl in clusters:
        arn = cl["DBClusterArn"]
        tags = list_rds_tags(rds, arn)
        name_tag = get_name_tag_value(tags)

        has_finops, finops_key, finops_raw, finops_type, finops_desc, finops_valid = get_finops_info(tags, ACTION)
        endpoint = cl.get("Endpoint") or ""
        cid = cl["DBClusterIdentifier"]

        result, err = "-", ""
        if has_finops:
            try:
                status = cl.get("Status")
                logger.debug(f"[RDS-Cluster] {cid} status={status} action={ACTION} finops_type={finops_type} valid={finops_valid}")

                if not finops_valid:
                    result = "value setting error"
                else:
                    do_exec, reason = should_execute(finops_type)
                    if not do_exec:
                        result = reason
                    else:
                        if ACTION == "start" and status in ("stopped", "stopping"):
                            rds.start_db_cluster(DBClusterIdentifier=cid)
                            result = "start: requested"
                        elif ACTION == "stop" and status in ("available", "modifying"):
                            rds.stop_db_cluster(DBClusterIdentifier=cid)
                            result = "stop: requested"
                        else:
                            result = f"no-op (status={status})"
            except ClientError as e:
                err = str(e)
                logger.exception(f"[RDS-Cluster] action failed {cid}")

            with_tag.append([
                now_kst_str(), "RDS-Cluster", name_tag, cid, endpoint, finops_raw or "", result, err
            ])
        else:
            without_tag.append([
                now_kst_str(), "RDS-Cluster", name_tag, cid, endpoint
            ])

    return with_tag, without_tag

# ---------- EKS ----------
def process_eks(region):
    eks = boto3.client("eks", region_name=region, config=BOTO_CONFIG)
    with_tag, without_tag = [], []

    clusters = eks.list_clusters().get("clusters", [])
    for cname in clusters:
        cdesc = eks.describe_cluster(name=cname).get("cluster", {})
        ctags = cdesc.get("tags", {}) or {}
        c_endpoint = cdesc.get("endpoint", "")

        ngs = eks.list_nodegroups(clusterName=cname).get("nodegroups", [])
        for ng in ngs:
            ngdesc = eks.describe_nodegroup(clusterName=cname, nodegroupName=ng).get("nodegroup", {})
            ngtags = ngdesc.get("tags", {}) or {}

            name_tag = get_name_tag_value(ngtags or ctags)

            # Only act when finops-* exists (nodegroup or cluster)
            has_finops, finops_key, finops_raw, finops_type, finops_desc, finops_valid = get_finops_info(ngtags or ctags, ACTION)

            rid = f"{cname}/{ng}"
            result, err = "-", ""

            if has_finops:
                try:
                    sc = ngdesc.get("scalingConfig", {}) or {}
                    minSize = int(sc.get("minSize", 0) or 0)
                    maxSize = int(sc.get("maxSize", 0) or 0)
                    currentDesired = int(sc.get("desiredSize", 0) or 0)

                    if not finops_valid:
                        result = "value setting error"
                    else:
                        do_exec, reason = should_execute(finops_type)
                        if not do_exec:
                            result = reason
                        else:
                            # --- desired target from ACTION ---
                            desired = EKS_DEFAULT_START_SIZE if ACTION == "start" else EKS_DEFAULT_STOP_SIZE
                            desired = int(desired)

                            # --- Clamp desired into [minSize, maxSize] to avoid API errors ---
                            # If maxSize is 0 (weird but possible), treat it as "no explicit upper bound" for clamping
                            if maxSize > 0 and desired > maxSize:
                                desired = maxSize
                            if desired < minSize:
                                desired = minSize
                            if desired < 0:
                                desired = 0

                            logger.debug(
                                f"[EKS] {rid} scaling clamp: min={minSize}, desired(target)={desired}, max={maxSize}, current={currentDesired}"
                            )

                            eks.update_nodegroup_config(
                                clusterName=cname,
                                nodegroupName=ng,
                                scalingConfig={"minSize": minSize, "maxSize": maxSize, "desiredSize": desired}
                            )
                            result = f"scale: desired={desired}"
                except ClientError as e:
                    err = str(e)
                    logger.exception(f"[EKS] scale failed {rid}")

                with_tag.append([
                    now_kst_str(), "EKS-NodeGroup", name_tag, rid, c_endpoint, finops_raw or "", result, err
                ])
            else:
                without_tag.append([
                    now_kst_str(), "EKS-NodeGroup", name_tag, rid, c_endpoint
                ])
    return with_tag, without_tag

# ---------- Handler ----------
def lambda_handler(event, context):
    global ACTION

    if not isinstance(event, dict):
        event = {}

    action = (event.get("ACTION") or ACTION).lower()
    regions_str = event.get("REGIONS") or ",".join(REGIONS)
    effective_regions = [r.strip() for r in regions_str.split(",") if r.strip()]

    ACTION = action
    logger.info(f"=== Start FinOps Lambda (ACTION={action}, REGIONS={effective_regions}) @ {now_kst_str()} KST ===")

    region_results = {}
    for region in effective_regions:
        try:
            logger.info(f"[{region}] scanning...")
            with_tag_all, without_tag_all = [], []

            wt, wot = process_ec2(region)
            with_tag_all.extend(wt); without_tag_all.extend(wot)

            wt, wot = process_rds(region)
            with_tag_all.extend(wt); without_tag_all.extend(wot)

            wt, wot = process_eks(region)
            with_tag_all.extend(wt); without_tag_all.extend(wot)

            region_results[region] = {"with": with_tag_all, "without": without_tag_all}
            logger.info(f"[{region}] done. with={len(with_tag_all)} without={len(without_tag_all)}")
        except Exception as e:
            logger.exception(f"[{region}] processing failed: {e}")

    # Slack: chunked messages
    try:
        msgs = build_slack_messages(region_results)
        send_chunks(msgs)
    except Exception as e:
        logger.exception(f"Slack build/send failed: {e}")

    logger.info(f"=== End FinOps Lambda @ {now_kst_str()} KST ===")
    return {"ok": True, "action": ACTION, "regions": effective_regions}

