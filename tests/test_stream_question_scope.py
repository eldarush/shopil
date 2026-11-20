"""Streaming scope tests: selected snapshot only, no absence-to-zero inference."""
from copy import deepcopy
import pytest
from chat.hybrid import Narrative, render_narrative, semantic_catalog
from chat.question_scope import recognize_question, scoped_entries


def source(table, state, rows):
    return {"table":table,"metric":"fixture stream metric; bounded snapshot", "units":"integer agorot and counts",
            "identifiers": ["window_start_ms"] + (["product_id"] if any("product_id" in row for row in rows) else []),
            "time_start":"2026-11-20T10:00:00Z","time_end":"2026-11-20T10:02:00Z",
            "checkpoint": 5,"watermark":"2026-11-20T10:00:45Z","window_state":state,
            "rows":[{"window_start":"2026-11-20T10:01:00Z","window_end":"2026-11-20T10:02:00Z",**row} for row in rows]}


def evidence():
    return {"snapshot_id":"stream-snapshot","captured_at":"2026-11-20T12:00:00Z","sources":[
        source("order_state","open",[{"window_start_ms":2000,"orders":2,"revenue_ils":"12.00"},
                                        {"window_start_ms":1000,"orders":9,"revenue_ils":"99.00"}]),
        source("realtime_revenue","closed",[{"window_start_ms":0,"orders":3,"revenue_ils":"20.00"}]),
        source("review_state","open",[{"window_start_ms":2000,"product_id":5,"reviews":5,"negative_reviews":4},
                                         {"window_start_ms":2000,"product_id":6,"reviews":2,"negative_reviews":1}]),
        source("review_alerts","open",[{"window_start_ms":2000,"product_id":5,"negative_reviews_at_trigger":3}]),
    ]}


@pytest.mark.parametrize("question,metric,state,product",[
    ("מה סכום אירועי ההזמנה בחלון הפתוח האחרון?","stream_order_amount","open",None),
    ("מהו מספר אירועי ההזמנה בחלון הסגור האחרון שמופיע בנתונים?","stream_order_count","closed",None),
    ("מה מספר הביקורות השליליות בזרם למוצר5 בחלון הפתוח האחרון?","stream_negative_reviews","open",5),
    ("מה מספר הביקורות השליליות בעת ההתראה בזרם של מוצר 5 בחלון הפתוח האחרון?","stream_alert_trigger_count","open",5),
])
def test_stream_forms_are_explicit(question,metric,state,product):
    scope=recognize_question(question,[])
    assert (scope.metric,scope.window_state,scope.product_id)==(metric,state,product)


def test_latest_window_selected_by_time_not_row_order_and_no_paid_relabeling():
    data=evidence(); query="מה סכום אירועי ההזמנה בחלון הפתוח האחרון?"
    scope=recognize_question(query,[]); entries=scoped_entries(semantic_catalog(data),scope)
    assert len(entries)==1
    ref=next(iter(entries)); assert entries[ref][1]["revenue_ils"]=="12.00"
    plan=Narrative(status="answer",sentences=[f"סכום אירועי ההזמנה בשקלים הוא {{{{{ref}}}}}."],explanation="stream_not_paid")
    answer=render_narrative(plan,data,entries,scope)
    assert "12.00" in answer and "99.00" not in answer
    assert "ללא מצב תשלום" in answer and "פתוח וזמני" in answer
    assert "מבין החלונות המתאימים שנכללו בצילום" in answer
    assert "חלון האירוע שנבחר" in answer and "לא כולל" in answer
    assert "מועד צילום הנתונים: 2026-11-20T12:00:00Z" in answer


def test_current_negative_count_and_alert_trigger_count_stay_distinct():
    data=evidence()
    q="מה מספר הביקורות השליליות בזרם למוצר5 בחלון הפתוח האחרון?"
    scope=recognize_question(q,[]); current=scoped_entries(semantic_catalog(data),scope)
    assert next(iter(current.values()))[1]["negative_reviews"]==4
    alertq="מה מספר הביקורות השליליות בעת ההתראה בזרם למוצר5 בחלון הפתוח האחרון?"
    scope=recognize_question(alertq,[]); alerts=scoped_entries(semantic_catalog(data),scope)
    ref=next(iter(alerts)); assert alerts[ref][1]["negative_reviews_at_trigger"]==3
    plan=Narrative(status="answer",sentences=[f"מספר הביקורות השליליות בעת ההתראה הוא {{{{{ref}}}}}."],explanation="alert_trigger_not_current")
    answer=render_narrative(plan,data,alerts,scope)
    assert "אינו המונה הנוכחי" in answer and "התאמתו למזהי המוצרים ההיסטוריים לא אומתה" in answer


def test_no_fallback_to_older_product_or_alert_and_no_empty_row_means_zero():
    data=evidence()
    data["sources"][2]["rows"].append({"window_start_ms":3000,"product_id":6,"reviews":1,"negative_reviews":0})
    for query in ["מה מספר הביקורות השליליות בזרם למוצר5 בחלון הפתוח האחרון?",
                  "מה מספר הביקורות השליליות בעת ההתראה בזרם למוצר5 בחלון הפתוח האחרון?",
                  "מה מספר אירועי ההזמנה בחלון הפתוח האחרון?"]:
        assert scoped_entries(semantic_catalog(data),recognize_question(query,[]))=={}


def test_stream_followup_preserves_stream_namespace_and_window_state():
    query="מה מספר הביקורות השליליות בזרם למוצר5 בחלון הפתוח האחרון?"
    history=[{"role":"user","content":query},{"role":"assistant","content":"historical product mapping is unverified"}]
    scope=recognize_question("ומה לגבי מוצר6?",history)
    assert scope.metric=="stream_negative_reviews" and scope.window_state=="open"
    selected=scoped_entries(semantic_catalog(evidence()),scope)
    assert next(iter(selected.values()))[1]["negative_reviews"]==1


@pytest.mark.parametrize("question",[
    "מה ההכנסה ששולמה בזרם?", "מה סכום אירועי ההזמנה בשעה האחרונה?",
    "מה סכום אירועי ההזמנה של מוצר5 בחלון הפתוח האחרון?",
    "מה מספר הביקורות השליליות בזרם בחלון הפתוח האחרון?",
    "מה סכום אירועי ההזמנה בחלון הפתוח האחרון וגם ההכנסה ההיסטורית?",
])
def test_unsupported_stream_semantics_are_not_guessed(question):
    assert recognize_question(question,[]) is None


def test_real_unique_event_during_mocked_model_keeps_snapshot_and_next_question_updates(tmp_path):
    """Real SQLite engine/event; mock language model. Not live-model experiment proof."""
    import asyncio
    from datetime import datetime, timezone
    import json
    import os
    from pathlib import Path
    import subprocess
    from types import SimpleNamespace
    from uuid import uuid4
    from chat.capture import capture_stream
    from chat.critic import Critic
    from chat.hybrid import HybridActor
    from chat.service import ChatService

    root=Path(__file__).resolve().parents[1]
    executable=root/"streaming/.venv"/("Scripts/python.exe" if os.name=="nt" else "bin/python")
    database=tmp_path/"stream.db"
    script='''
import sys
from streaming_lab import StreamEngine
from streaming_lab.events import Event
with StreamEngine(sys.argv[1]) as engine:
    offset=engine.next_offset
    event=Event("fresh-"+str(offset), "order", "2026-11-20T10:00:01+00:00", 5, amount_cents=100)
    print(engine.ingest(event,offset=offset)["status"])
'''
    def insert():
        result=subprocess.run([str(executable),"-c",script,str(database)],cwd=root,capture_output=True,text=True,check=True)
        assert result.stdout.strip()=="accepted"
    insert()
    payloads=[]; inserted=False
    async def capture(question,history):
        stream=await capture_stream(database)
        return {"snapshot_id":uuid4().hex,"captured_at":datetime.now(timezone.utc).isoformat(),
                "sources":[source.model_dump(mode="json") for source in stream["sources"]],"stream":stream["stream"]}
    async def complete(messages,*,schema=None):
        nonlocal inserted
        payloads.append(json.loads(messages[2]["content"]))
        if "feedback" in schema.get("properties",{}):
            return '{"feedback":"Fixture verified", "is_acceptable":true}'
        if not inserted:
            insert()  # A genuinely unique accepted event AFTER Actor received evidence.
            inserted=True
        option=schema["anyOf"][0]["properties"]["sentences"]["items"]["enum"][0]
        return json.dumps({"status":"answer","sentences":[option],"explanation":"open_window_provisional"},ensure_ascii=False)
    model=SimpleNamespace(complete=complete); audit=[]
    service=ChatService(HybridActor(model),Critic(model),capture,audit.append)
    query="מה מספר אירועי ההזמנה בחלון הפתוח האחרון?"
    first=asyncio.run(service.answer(query,[]))
    second=asyncio.run(service.answer(query,first.history))
    assert first.status==second.status=="approved"
    assert payloads[0]==payloads[1] and payloads[2]==payloads[3]
    def count(payload):
        return next(source for source in payload["sources"] if source["table"]=="order_state")["rows"][0]["orders"]
    assert count(payloads[0])==1 and count(payloads[2])==2
    assert payloads[0]["snapshot_id"] != payloads[2]["snapshot_id"]
    assert payloads[2]["stream"]["checkpoint"] > payloads[0]["stream"]["checkpoint"]


def test_open_stream_rationale_choices_differ_without_removing_mandatory_caveats():
    from chat.model import ModelError
    data = evidence()
    scope = recognize_question("מה מספר אירועי ההזמנה בחלון הפתוח האחרון?", [])
    entries = scoped_entries(semantic_catalog(data), scope)
    schema = Narrative.generation_schema(entries)["anyOf"][0]
    assert set(schema["properties"]["explanation"]["enum"]) == {
        "open_window_provisional", "stream_snapshot_boundary", "stream_not_paid"}
    sentence = schema["properties"]["sentences"]["items"]["enum"][0]
    def render_with(explanation):
        return render_narrative(Narrative(status="answer", sentences=[sentence], explanation=explanation),
                                data, entries, scope)
    provisional = render_with("open_window_provisional")
    progress = render_with("stream_snapshot_boundary")
    assert "עשויים לשנות את הערך" in provisional and "עשויים לשנות את הערך" not in progress
    assert "אירועים שעובדו לאחר הצילום" in progress and "אירועים שעובדו לאחר הצילום" not in provisional
    for answer in (provisional, progress):
        for caveat in ("פתוח וזמני", "ללא מצב תשלום", "מבין החלונות המתאימים", "לא כולל"):
            assert caveat in answer
    for incompatible in ("none", "closed_window_not_all_history", "alert_trigger_not_current"):
        with pytest.raises(ModelError, match="rationale"):
            render_with(incompatible)


def test_scope_guidance_exact_bindings_and_distinct_nonanswer_states():
    from chat.hybrid import scope_guidance
    data = evidence()
    scope = recognize_question("מה מספר אירועי ההזמנה בחלון הפתוח האחרון?", [])
    entries = scoped_entries(semantic_catalog(data), scope)
    hint = scope_guidance(scope, entries, None)
    assert hint["answerability"] == "grounded_fact_available"
    assert hint["metric"] == "stream_order_count"
    assert hint["bindings"] == [{"reference": next(iter(entries)), "field": "orders", "table": "order_state"}]
    assert scope_guidance(scope, {}, None)["answerability"] == "no_matching_fact_in_scope"
    assert scope_guidance(None, {}, None) == {"answerability": "unsupported_question"}
    shipping = recognize_question("מהן עלויות המשלוח?", [])
    assert scope_guidance(shipping, {}, None)["answerability"] == "no_matching_fact_in_scope"
    assert scope_guidance(shipping, {}, "shipping_cost") == {
        "answerability": "capture_supported_missing_metric", "metric": "shipping_cost"}
