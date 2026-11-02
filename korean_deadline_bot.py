"""
프로젝트 마감 관리 봇 (한국어 명령어 / 분야별 담당자·마감일 지원 / 완료 처리 지원)

- /프로젝트등록   : (봇 관리자 전용) 프로젝트를 이름으로 등록. 이 명령어를 친 채널이 이 프로젝트의 전용 알림방이 됨.
                    최종 마감일(영상 업로드일)을 등록 시점에 바로 지정 가능 (선택).
- /분야등록      : (누구나 가능) 프로젝트 안에 분야(일러스트/영상/디자인/믹싱)를 추가하고 담당자+마감일 지정.
                    담당자 본인이 직접 등록해도 알림을 받을 수 있음.
- /작업완료      : (해당 분야 담당자 본인 또는 봇 관리자) 분야 작업을 완료 처리. 완료되면 그 마감일까지 더는 알림이 안 감.
- /분야삭제      : 특정 분야만 삭제
- /프로젝트목록   : 전체 프로젝트와 각 분야별 담당자/마감일/남은 일수/완료 여부 확인
- /프로젝트삭제   : (봇 관리자 전용) 프로젝트 전체 삭제 (모든 분야 포함)

- /메시지보내기   : (봇 관리자 전용) 지금 바로 원하는 채널 또는 특정 사람 DM으로 메시지 발송
- /예약발송      : (봇 관리자 전용) 지정한 날짜·시각(한국시간)에 채널 또는 DM으로 메시지 발송 (매일/매주 반복 가능)
- /예약목록, /예약삭제 : (봇 관리자 전용) 예약된 메시지 확인 / 취소

동작 방식:
- 매일 낮 12시(한국시간)에 완료되지 않은 분야(+ 담당자 미정인 최종 마감일)를 확인해서, 마감일까지 남은 일수가
  14일 / 5일 / 3일 / 2일 / 1일 / 0일(당일) 이 되면 알려줍니다.
- 알림은 항상 그 프로젝트를 `/프로젝트등록` 했던 채널로만 갑니다 (다른 채널에서 /분야등록 해도 안 바뀜).
- 분야마다 독립적으로 알림 주기가 돌아갑니다.
- 한 번 보낸 알림은 다시 보내지 않도록 기록해둡니다.
- `/작업완료` 로 완료 처리된 분야는 그 마감일 기준 알림이 더는 오지 않습니다 (마감일을 다시 등록하면 초기화됨).
- 봇이 잠깐 꺼져 있었다가 다시 켜져도, 놓친 알림은 다음 체크 때 한 번에 보내줍니다.

권한:
- "봇 관리자"는 이 봇 애플리케이션을 Discord Developer Portal에서 만든 계정(=봇을 소유한 사람)입니다.
  discord.py 가 자동으로 판별하므로 별도 설정이 필요 없습니다.

데이터 저장:
- 환경변수 UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN 이 설정되어 있으면
  Upstash Redis(무료, 영구 보존)에 저장합니다. Render 같은 클라우드에 올릴 때 이걸 씁니다.
- 두 환경변수가 없으면 로컬 파일(projects.json)에 저장합니다. 내 컴퓨터에서 테스트할 때 이렇게 동작합니다.

실행 전 준비:
1. pip install -r requirements.txt
2. 환경변수 DISCORD_TOKEN 에 봇 토큰 설정
3. Discord Developer Portal의 Bot 탭에서 "Server Members Intent" 켜기
4. python korean_deadline_bot.py
"""

import os
import json
import asyncio
import threading
from datetime import datetime, timedelta, timezone, date, time as dtime

import requests
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask

# ────────────────────────────────
# 기본 설정
# ────────────────────────────────
DATA_FILE = "projects.json"
KST = timezone(timedelta(hours=9))  # 한국 시간
NOTIFY_TIME = dtime(hour=12, minute=0, tzinfo=KST)  # 매일 마감 알림을 체크할 시각 (한국시간 낮 12시)
MILESTONES = [14, 5, 3, 2, 1, 0]  # 마감 며칠 전에 알려줄지 (0 = 당일)

# 고정 분야 목록. "영상"의 마감일이 곧 프로젝트의 최종 마감일(=업로드일)로 취급됩니다.
FIELD_CHOICES = ["일러스트", "영상", "디자인", "믹싱"]
FINAL_FIELD = "영상"

intents = discord.Intents.default()
intents.members = True  # 담당자 태그를 위해 필요
bot = commands.Bot(command_prefix="!", intents=intents)


# ────────────────────────────────
# Render.com 무료 호스팅용 "저 살아있어요" 웹 서버
# (Render 무료 웹 서비스는 HTTP 요청이 없으면 잠드는데,
#  UptimeRobot 같은 서비스가 이 주소를 주기적으로 핑 해주면 계속 깨어있게 됨)
# ────────────────────────────────
keep_alive_app = Flask(__name__)


@keep_alive_app.route("/")
def keep_alive_home():
    return "봇 살아있어요 🤖"


def run_keep_alive_server():
    port = int(os.environ.get("PORT", 8080))
    keep_alive_app.run(host="0.0.0.0", port=port)


# ────────────────────────────────
# 데이터 저장/불러오기
# 구조: { "projects": [ { "name": "A", "channel_id": ..,
#           "fields": [ { "field": "일러스트", "assignee_id": .., "deadline": "2026-10-01", "notified": [] } ] } ] }
#
# 저장 위치: UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN 환경변수가 설정되어 있으면
# 그 클라우드 DB(Upstash Redis, 무료/영구 보존)에 저장하고, 없으면(=로컬 컴퓨터에서 테스트할 때)
# 그냥 로컬 파일(projects.json)에 저장합니다.
# ────────────────────────────────
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
REDIS_KEY = "discord_deadline_bot_projects"


def _upstash_command(cmd_list):
    resp = requests.post(
        UPSTASH_URL,
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        json=cmd_list,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("result")


def load_data():
    if UPSTASH_URL and UPSTASH_TOKEN:
        try:
            result = _upstash_command(["GET", REDIS_KEY])
            if result:
                return json.loads(result)
            return {"projects": []}
        except Exception as e:
            print(f"⚠️ Upstash에서 불러오기 실패, 빈 데이터로 시작합니다: {e}")
            return {"projects": []}

    if not os.path.exists(DATA_FILE):
        return {"projects": []}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    if UPSTASH_URL and UPSTASH_TOKEN:
        try:
            payload = json.dumps(data, ensure_ascii=False)
            _upstash_command(["SET", REDIS_KEY, payload])
        except Exception as e:
            print(f"⚠️ Upstash에 저장 실패: {e}")
        return

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


data = load_data()


def find_project(name: str):
    for p in data["projects"]:
        if p["name"].lower() == name.lower():
            return p
    return None


def find_field(project, field_name: str):
    for f in project["fields"]:
        if f["field"].lower() == field_name.lower():
            return f
    return None


def parse_date(text: str) -> date | None:
    """'2026-10-01' 또는 '10-01' 형식 지원"""
    text = text.strip()
    for fmt in ("%Y-%m-%d", "%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%m-%d":
                today = datetime.now(KST)
                parsed = parsed.replace(year=today.year)
                if parsed.date() < today.date():
                    parsed = parsed.replace(year=today.year + 1)
            return parsed.date()
        except ValueError:
            continue
    return None


def parse_hhmm(text: str) -> dtime | None:
    """'14:30' 형식 (24시간제)"""
    try:
        h, m = text.strip().split(":")
        return dtime(hour=int(h), minute=int(m))
    except (ValueError, TypeError):
        return None


# ────────────────────────────────
# 권한 체크: 봇 관리자(=봇을 만든 개발자 계정) 전용 명령어에 사용
# ────────────────────────────────
async def owner_check(interaction: discord.Interaction) -> bool:
    if await interaction.client.is_owner(interaction.user):
        return True
    await interaction.response.send_message("🚫 이 명령어는 봇 관리자만 사용할 수 있어요.", ephemeral=True)
    return False


def owner_only():
    return app_commands.check(owner_check)


# ────────────────────────────────
# 슬래시 명령어 (한국어)
# ────────────────────────────────
@bot.tree.command(name="프로젝트등록", description="(봇 관리자 전용) 새 프로젝트를 등록합니다. 이 명령어를 친 채널이 전용 알림방이 됩니다.")
@owner_only()
@app_commands.describe(
    project="프로젝트 이름 (예: A)",
    deadline="영상 업로드일 = 최종 마감일 (선택. 나중에 /분야등록 으로도 지정 가능) 예: 2026-10-01",
)
async def register_project(interaction: discord.Interaction, project: str, deadline: str = None):
    if find_project(project):
        await interaction.response.send_message(f"⚠️ '{project}' 프로젝트는 이미 등록되어 있어요.", ephemeral=True)
        return

    final_deadline = None
    if deadline:
        due = parse_date(deadline)
        if due is None:
            await interaction.response.send_message(
                "⚠️ 날짜 형식이 올바르지 않아요. 예: `2026-10-01` 또는 `10-01`", ephemeral=True
            )
            return
        final_deadline = due.isoformat()

    data["projects"].append(
        {
            "name": project,
            "channel_id": interaction.channel_id,  # 이 프로젝트의 알림은 항상 이 채널로만 감
            "final_deadline": final_deadline,
            "final_notified": [],
            "fields": [],
        }
    )
    await asyncio.to_thread(save_data, data)

    msg = f"📁 프로젝트 **{project}** 등록 완료! 이 채널로 앞으로 이 프로젝트 관련 알림이 올라와요."
    if final_deadline:
        days_left = (due - datetime.now(KST).date()).days
        msg += f"\n🎬 최종 마감(업로드일): **{final_deadline}** (D-{days_left})"
    msg += "\n분야별 담당자·마감일은 `/분야등록` 으로 추가해주세요."

    await interaction.response.send_message(msg)


@bot.tree.command(name="분야등록", description="프로젝트 안에 분야를 추가하고 담당자·마감일을 지정합니다.")
@app_commands.describe(
    project="프로젝트 이름",
    field="분야 선택",
    assignee="담당자",
    deadline="마감일 (예: 2026-10-01 또는 10-01)",
)
@app_commands.choices(
    field=[app_commands.Choice(name=f, value=f) for f in FIELD_CHOICES]
)
async def register_field(
    interaction: discord.Interaction,
    project: str,
    field: app_commands.Choice[str],
    assignee: discord.Member,
    deadline: str,
):
    field_name = field.value

    p = find_project(project)
    if not p:
        await interaction.response.send_message(
            f"❌ '{project}' 프로젝트를 찾을 수 없어요. 먼저 `/프로젝트등록` 으로 등록해주세요.", ephemeral=True
        )
        return

    due = parse_date(deadline)
    if due is None:
        await interaction.response.send_message(
            "⚠️ 날짜 형식이 올바르지 않아요. 예: `2026-10-01` 또는 `10-01`", ephemeral=True
        )
        return

    existing = find_field(p, field_name)
    if existing:
        # 이미 있는 분야면 담당자/마감일 갱신 + 알림 기록·완료 상태 초기화
        existing["assignee_id"] = assignee.id
        existing["deadline"] = due.isoformat()
        existing["notified"] = []
        existing["completed"] = False
        action = "수정"
    else:
        p["fields"].append(
            {
                "field": field_name,
                "assignee_id": assignee.id,
                "deadline": due.isoformat(),
                "notified": [],
                "completed": False,
            }
        )
        action = "추가"

    p["channel_id"] = p.get("channel_id", interaction.channel_id)  # 알림 채널은 프로젝트 등록 당시 채널로 고정
    await asyncio.to_thread(save_data, data)

    days_left = (due - datetime.now(KST).date()).days
    final_note = " (=최종 마감일 / 업로드일)" if field_name == FINAL_FIELD else ""
    home_channel = bot.get_channel(p["channel_id"])
    channel_note = f" ({home_channel.mention} 채널로 알림이 가요)" if home_channel else ""
    await interaction.response.send_message(
        f"✅ **{project} - {field_name}**{final_note} 분야 {action} 완료!{channel_note}\n"
        f"담당자: {assignee.mention} | 마감일: **{due.isoformat()}** (D-{days_left})\n"
        f"마감 14일 전 / 5일 전 / 3일 전 / 2일 전 / 1일 전 / 당일에 자동으로 태그해드릴게요."
    )


@bot.tree.command(name="작업완료", description="본인이 맡은 분야 작업을 완료 처리합니다. 완료하면 마감일 전까지 알림이 더 안 갑니다.")
@app_commands.describe(project="프로젝트 이름", field="완료 처리할 분야")
@app_commands.choices(
    field=[app_commands.Choice(name=f, value=f) for f in FIELD_CHOICES]
)
async def complete_field(interaction: discord.Interaction, project: str, field: app_commands.Choice[str]):
    field_name = field.value

    p = find_project(project)
    if not p:
        await interaction.response.send_message(f"❌ '{project}' 프로젝트를 찾을 수 없어요.", ephemeral=True)
        return

    f = find_field(p, field_name)
    if not f:
        await interaction.response.send_message(f"❌ '{project}'에 '{field_name}' 분야가 없어요.", ephemeral=True)
        return

    is_assignee = f["assignee_id"] == interaction.user.id
    if not is_assignee and not await bot.is_owner(interaction.user):
        await interaction.response.send_message(
            "🚫 본인이 담당한 분야만 완료 처리할 수 있어요.", ephemeral=True
        )
        return

    if f.get("completed"):
        await interaction.response.send_message(
            f"이미 **{project} - {field_name}** 는 완료 처리되어 있어요.", ephemeral=True
        )
        return

    f["completed"] = True
    await asyncio.to_thread(save_data, data)
    await interaction.response.send_message(
        f"🎉 **{project} - {field_name}** 작업 완료 처리했어요! 이 마감일 기준 알림은 더 이상 오지 않아요."
    )


@bot.tree.command(name="분야삭제", description="프로젝트에서 특정 분야만 삭제합니다.")
@app_commands.describe(project="프로젝트 이름", field="삭제할 분야")
@app_commands.choices(
    field=[app_commands.Choice(name=f, value=f) for f in FIELD_CHOICES]
)
async def remove_field(interaction: discord.Interaction, project: str, field: app_commands.Choice[str]):
    field_name = field.value

    p = find_project(project)
    if not p:
        await interaction.response.send_message(f"❌ '{project}' 프로젝트를 찾을 수 없어요.", ephemeral=True)
        return

    f = find_field(p, field_name)
    if not f:
        await interaction.response.send_message(f"❌ '{project}'에 '{field_name}' 분야가 없어요.", ephemeral=True)
        return

    p["fields"].remove(f)
    await asyncio.to_thread(save_data, data)
    await interaction.response.send_message(f"🗑️ **{project} - {field_name}** 분야 삭제했어요.")


@bot.tree.command(name="프로젝트목록", description="등록된 모든 프로젝트와 분야별 현황을 봅니다.")
async def list_projects(interaction: discord.Interaction):
    if not data["projects"]:
        await interaction.response.send_message("등록된 프로젝트가 없어요.", ephemeral=True)
        return

    today = datetime.now(KST).date()
    blocks = []
    for p in data["projects"]:
        header = f"**📁 {p['name']}**"
        final_field = find_field(p, FINAL_FIELD)
        if final_field:
            due = date.fromisoformat(final_field["deadline"])
            days_left = (due - today).days
            status = f"D-{days_left}" if days_left >= 0 else f"⚠️ {abs(days_left)}일 지남"
            done = " | ✅ 완료" if final_field.get("completed") else ""
            header += f"  🎬 최종 마감(업로드일): {final_field['deadline']} ({status}){done}"
        elif p.get("final_deadline"):
            due = date.fromisoformat(p["final_deadline"])
            days_left = (due - today).days
            status = f"D-{days_left}" if days_left >= 0 else f"⚠️ {abs(days_left)}일 지남"
            header += f"  🎬 최종 마감(업로드일): {p['final_deadline']} ({status}, 담당자 미정)"

        lines = [header]
        if not p["fields"]:
            lines.append("  (등록된 분야 없음 — `/분야등록` 으로 추가해주세요)")
        for f in p["fields"]:
            due = date.fromisoformat(f["deadline"])
            days_left = (due - today).days
            status = f"D-{days_left}" if days_left >= 0 else f"⚠️ {abs(days_left)}일 지남"
            mark = " ⭐" if f["field"] == FINAL_FIELD else ""
            done = "✅ 완료" if f.get("completed") else "⏳ 진행중"
            lines.append(f"  ├ {f['field']}{mark} | <@{f['assignee_id']}> | {f['deadline']} ({status}) | {done}")
        blocks.append("\n".join(lines))

    await interaction.response.send_message("\n\n".join(blocks))


@bot.tree.command(name="프로젝트삭제", description="(봇 관리자 전용) 프로젝트 전체를 삭제합니다 (모든 분야 포함).")
@owner_only()
@app_commands.describe(project="삭제할 프로젝트 이름")
async def remove_project(interaction: discord.Interaction, project: str):
    p = find_project(project)
    if not p:
        await interaction.response.send_message(f"❌ '{project}' 프로젝트를 찾을 수 없어요.", ephemeral=True)
        return

    data["projects"].remove(p)
    await asyncio.to_thread(save_data, data)
    await interaction.response.send_message(f"🗑️ 프로젝트 **{project}** 전체 삭제했어요.")


# ────────────────────────────────
# 예약 메시지 / DM 발송 (봇 관리자 전용)
# 저장 형태: data["scheduled"] = [ {id, target_type("channel"/"user"), target_id,
#            message, due(한국시간, ISO), repeat("none"/"daily"/"weekly"), created_by} ]
# ────────────────────────────────
REPEAT_STEPS = {"daily": timedelta(days=1), "weekly": timedelta(days=7)}
REPEAT_LABELS = {"none": "반복 없음", "daily": "매일", "weekly": "매주"}


def _now_kst_naive() -> datetime:
    return datetime.now(KST).replace(tzinfo=None)


def _resolve_target(interaction: discord.Interaction, channel, user):
    """(target_type, target_id, 에러문구) 반환. 채널/사람 둘 다 비우면 명령어를 친 채널."""
    if channel and user:
        return None, None, "⚠️ 채널과 사람 중 **하나만** 지정해주세요."
    if user:
        return "user", user.id, None
    if channel:
        return "channel", channel.id, None
    return "channel", interaction.channel_id, None


def _target_label(target_type: str, target_id: int) -> str:
    return f"<@{target_id}> (DM)" if target_type == "user" else f"<#{target_id}>"


async def _deliver(target_type: str, target_id: int, message: str) -> bool:
    """실제로 메시지를 보냄. 성공하면 True. (\\n 은 줄바꿈으로 바꿔서 보냄)"""
    text = message.replace("\\n", "\n")
    try:
        if target_type == "user":
            user = bot.get_user(target_id) or await bot.fetch_user(target_id)
            await user.send(text)
        else:
            ch = bot.get_channel(target_id) or await bot.fetch_channel(target_id)
            await ch.send(text)
        return True
    except (discord.Forbidden, discord.NotFound, discord.HTTPException) as e:
        print(f"⚠️ 메시지 전송 실패 ({target_type} {target_id}): {e}")
        return False


@bot.tree.command(name="메시지보내기", description="(봇 관리자 전용) 지금 바로 채널 또는 DM으로 메시지를 보냅니다.")
@owner_only()
@app_commands.describe(
    message="보낼 내용 (줄바꿈은 \\n 으로 입력)",
    channel="보낼 채널 (비우면 지금 이 채널)",
    user="DM으로 보낼 사람 (채널 대신 DM)",
)
async def send_now(
    interaction: discord.Interaction,
    message: str,
    channel: discord.TextChannel = None,
    user: discord.User = None,
):
    target_type, target_id, err = _resolve_target(interaction, channel, user)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    ok = await _deliver(target_type, target_id, message)
    if ok:
        await interaction.followup.send(f"✅ {_target_label(target_type, target_id)} 로 보냈어요.", ephemeral=True)
    else:
        await interaction.followup.send(
            "❌ 보내지 못했어요. DM이라면 상대가 '서버 멤버의 DM 허용'을 꺼뒀거나 봇과 같은 서버에 없을 수 있고, "
            "채널이라면 봇에게 그 채널의 메시지 보내기 권한이 있는지 확인해주세요.",
            ephemeral=True,
        )


@bot.tree.command(name="예약발송", description="(봇 관리자 전용) 지정한 날짜·시각(한국시간)에 채널 또는 DM으로 메시지를 보냅니다.")
@owner_only()
@app_commands.describe(
    message="보낼 내용 (줄바꿈은 \\n 으로 입력)",
    day="날짜 예: 2026-10-01 또는 10-01",
    at="시각(24시간제) 예: 14:30",
    channel="보낼 채널 (비우면 지금 이 채널)",
    user="DM으로 보낼 사람 (채널 대신 DM)",
    repeat="반복 여부 (기본: 반복 없음)",
)
@app_commands.rename(day="date", at="time")
@app_commands.choices(
    repeat=[
        app_commands.Choice(name="반복 없음", value="none"),
        app_commands.Choice(name="매일", value="daily"),
        app_commands.Choice(name="매주", value="weekly"),
    ]
)
async def schedule_message(
    interaction: discord.Interaction,
    message: str,
    day: str,
    at: str,
    channel: discord.TextChannel = None,
    user: discord.User = None,
    repeat: app_commands.Choice[str] = None,
):
    target_type, target_id, err = _resolve_target(interaction, channel, user)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return

    due_date = parse_date(day)
    due_time = parse_hhmm(at)
    if due_date is None or due_time is None:
        await interaction.response.send_message(
            "⚠️ 날짜/시각 형식이 올바르지 않아요. 날짜 예: `2026-10-01` 또는 `10-01`, 시각 예: `14:30` (24시간제)",
            ephemeral=True,
        )
        return

    repeat_value = repeat.value if repeat else "none"
    due = datetime.combine(due_date, due_time)
    now = _now_kst_naive()
    if due <= now:
        step = REPEAT_STEPS.get(repeat_value)
        if step is None:
            await interaction.response.send_message("⚠️ 이미 지난 시각이에요. 미래 시각으로 지정해주세요.", ephemeral=True)
            return
        while due <= now:  # 반복 예약이면 다음 차례 시각으로 맞춤
            due += step

    schedule_id = data.get("next_sched_id", 1)
    data["next_sched_id"] = schedule_id + 1
    data.setdefault("scheduled", []).append(
        {
            "id": schedule_id,
            "target_type": target_type,
            "target_id": target_id,
            "message": message,
            "due": due.isoformat(),
            "repeat": repeat_value,
            "created_by": interaction.user.id,
        }
    )
    await asyncio.to_thread(save_data, data)

    await interaction.response.send_message(
        f"⏰ 예약 완료! (예약 ID: **{schedule_id}**)\n"
        f"받는 곳: {_target_label(target_type, target_id)}\n"
        f"보낼 시각: **{due:%Y-%m-%d %H:%M}** (한국시간) / {REPEAT_LABELS[repeat_value]}\n"
        f"내용: {message}",
        ephemeral=True,
    )


@bot.tree.command(name="예약목록", description="(봇 관리자 전용) 예약된 메시지 목록을 봅니다.")
@owner_only()
async def list_scheduled(interaction: discord.Interaction):
    items = sorted(data.get("scheduled", []), key=lambda s: s["due"])
    if not items:
        await interaction.response.send_message("예약된 메시지가 없어요.", ephemeral=True)
        return

    lines = []
    for s in items:
        due = datetime.fromisoformat(s["due"])
        preview = s["message"].replace("\\n", " ")
        if len(preview) > 40:
            preview = preview[:40] + "…"
        lines.append(
            f"`#{s['id']}` {due:%m/%d %H:%M} | {REPEAT_LABELS.get(s.get('repeat', 'none'), '반복 없음')} | "
            f"{_target_label(s['target_type'], s['target_id'])} | {preview}"
        )

    text = "\n".join(lines)
    if len(text) > 1900:
        text = text[:1900] + "\n…(너무 길어서 일부만 표시)"
    await interaction.response.send_message(text, ephemeral=True)


@bot.tree.command(name="예약삭제", description="(봇 관리자 전용) 예약된 메시지를 취소합니다.")
@owner_only()
@app_commands.describe(schedule_id="취소할 예약 ID (/예약목록 으로 확인)")
@app_commands.rename(schedule_id="id")
async def cancel_scheduled(interaction: discord.Interaction, schedule_id: int):
    items = data.get("scheduled", [])
    target = next((s for s in items if s["id"] == schedule_id), None)
    if not target:
        await interaction.response.send_message("❌ 해당 ID의 예약을 찾을 수 없어요.", ephemeral=True)
        return

    items.remove(target)
    await asyncio.to_thread(save_data, data)
    await interaction.response.send_message(f"🗑️ 예약 `#{schedule_id}` 취소했어요.", ephemeral=True)


# ────────────────────────────────
# 백그라운드 루프 ①: 매일 낮 12시(한국시간) 마감 체크 (분야별로 독립 동작)
# ────────────────────────────────
@tasks.loop(time=NOTIFY_TIME)
async def check_deadlines():
    today = datetime.now(KST).date()
    changed = False

    for p in data["projects"]:
        channel = bot.get_channel(p["channel_id"])
        video_field = find_field(p, FINAL_FIELD)

        # 담당자가 아직 지정 안 된 최종 마감일(영상 업로드일)도 챙겨서 알림
        if not video_field and p.get("final_deadline"):
            due = date.fromisoformat(p["final_deadline"])
            days_left = (due - today).days
            notified = p.setdefault("final_notified", [])

            for m in sorted(MILESTONES, reverse=True):
                if days_left <= m and m not in notified:
                    if channel:
                        label = "오늘이 최종 마감(업로드일)입니다!" if m == 0 else f"최종 마감(업로드일) {m}일 전이에요"
                        try:
                            await channel.send(
                                f"📢 **{p['name']}** {label} (마감일: {p['final_deadline']})\n"
                                f"담당자가 아직 지정 안 됐어요 — `/분야등록`으로 영상 담당자를 지정해주세요!"
                            )
                        except discord.HTTPException:
                            pass
                    notified.append(m)
                    changed = True

        for f in p["fields"]:
            if f.get("completed"):
                continue  # 완료 처리된 분야는 알림 생략

            due = date.fromisoformat(f["deadline"])
            days_left = (due - today).days

            for m in sorted(MILESTONES, reverse=True):
                if days_left <= m and m not in f["notified"]:
                    if channel:
                        label = "오늘 마감입니다!" if m == 0 else f"마감 {m}일 전이에요"
                        try:
                            await channel.send(
                                f"📢 <@{f['assignee_id']}> **{p['name']} - {f['field']}** {label} "
                                f"(마감일: {f['deadline']})"
                            )
                        except discord.HTTPException:
                            pass
                    f["notified"].append(m)
                    changed = True

    if changed:
        await asyncio.to_thread(save_data, data)


@check_deadlines.before_loop
async def before_check_deadlines():
    await bot.wait_until_ready()


# ────────────────────────────────
# 백그라운드 루프 ②: 1분마다 예약 메시지 확인 (시각이 된 것만 발송)
# ────────────────────────────────
@tasks.loop(minutes=1)
async def check_scheduled():
    now = _now_kst_naive()
    changed = False

    for s in list(data.get("scheduled", [])):  # 복사본으로 순회 (도중에 목록이 바뀌어도 안전)
        due = datetime.fromisoformat(s["due"])
        if due > now:
            continue

        ok = await _deliver(s["target_type"], s["target_id"], s["message"])

        # 성공/실패와 관계없이 다음 단계로 넘어감 (실패한 걸 1분마다 무한 재시도하지 않도록)
        step = REPEAT_STEPS.get(s.get("repeat", "none"))
        if step:
            while due <= now:  # 봇이 꺼져 있던 동안 놓친 회차는 건너뛰고 다음 회차로
                due += step
            s["due"] = due.isoformat()
        elif s in data.get("scheduled", []):
            data["scheduled"].remove(s)
        changed = True

        if not ok:  # 실패하면 예약을 만든 사람에게 알려줌
            await _deliver(
                "user",
                s["created_by"],
                f"⚠️ 예약 메시지 #{s['id']} 를 {_target_label(s['target_type'], s['target_id'])} 로 보내지 못했어요. "
                f"(DM 차단 또는 채널 권한 문제일 수 있어요)",
            )

    if changed:
        await asyncio.to_thread(save_data, data)


@check_scheduled.before_loop
async def before_check_scheduled():
    await bot.wait_until_ready()


# ────────────────────────────────
# 슬래시 명령어 오류 처리 (권한 체크 실패는 이미 위에서 응답했으므로 조용히 무시)
# ────────────────────────────────
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        return
    print(f"⚠️ 명령어 처리 중 오류: {error}")
    if not interaction.response.is_done():
        try:
            await interaction.response.send_message("⚠️ 처리 중 오류가 발생했어요.", ephemeral=True)
        except discord.HTTPException:
            pass


# ────────────────────────────────
# 봇 시작 이벤트
# ────────────────────────────────
@bot.event
async def on_ready():
    await bot.tree.sync()
    # on_ready 는 재연결될 때마다 또 호출될 수 있어서, 이미 돌고 있으면 다시 시작하지 않음
    if not check_deadlines.is_running():
        check_deadlines.start()
    if not check_scheduled.is_running():
        check_scheduled.start()
    print(f"✅ 로그인 완료: {bot.user} (ID: {bot.user.id})")
    print("슬래시 명령어 동기화 완료. /프로젝트등록(관리자), /분야등록, /작업완료, /프로젝트목록, /분야삭제, /프로젝트삭제(관리자), "
          "/메시지보내기·/예약발송·/예약목록·/예약삭제(관리자) 사용 가능")


if __name__ == "__main__":
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        raise SystemExit(
            "❌ DISCORD_TOKEN 환경변수가 설정되지 않았어요.\n"
            "터미널에서: set DISCORD_TOKEN=여기에_토큰_붙여넣기 (Windows)\n"
            "또는 Mac/Linux: export DISCORD_TOKEN=여기에_토큰_붙여넣기"
        )

    # Render 등 클라우드에 올렸을 때만 keep-alive 서버를 켬 (PORT 환경변수가 있으면 클라우드로 간주)
    if os.environ.get("PORT"):
        threading.Thread(target=run_keep_alive_server, daemon=True).start()

    bot.run(token)
