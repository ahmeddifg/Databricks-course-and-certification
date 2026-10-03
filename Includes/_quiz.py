# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Interactive quiz engine
# MAGIC Included by the `questions` notebooks with `%run ../../Includes/_quiz`.
# MAGIC
# MAGIC `render_quiz(questions, title)` draws a clickable exam-style quiz (instant feedback, explanations,
# MAGIC score and per-topic breakdown) with `displayHTML`. No Spark job is started.
# MAGIC
# MAGIC `render_quiz(questions, title, mode="exam", minutes=90)` is the **exam simulation**: no feedback until you click
# MAGIC **Submit exam** (or the countdown reaches 0), then every answer, explanation and the per-topic score are shown.
# MAGIC
# MAGIC Question format:
# MAGIC ```python
# MAGIC {"topic": "Compute", "q": "Question text with `code` and **bold**",
# MAGIC  "code": "optional code block", "options": ["A", "B", "C", "D"],
# MAGIC  "answer": 1,               # index (0-based) - or a list of indexes for "choose TWO"
# MAGIC  "explanation": "Why the answer is right and the others are wrong"}
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Quiz renderer
import json as _json
import uuid as _uuid

_QUIZ_TEMPLATE = r"""
<meta charset="utf-8">
<style>
.qz{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:#fff;
  color:#1d2433;padding:16px 18px;border-radius:14px;border:1px solid #e3e8ef;max-width:1000px}
.qz h2{margin:0 0 4px 0;color:#0b2a4a;font-size:22px}
.qz .meta{color:#5b6576;font-size:13px;margin-bottom:10px}
.qz .bar{position:sticky;top:0;background:#0b2a4a;color:#fff;border-radius:10px;padding:8px 12px;
  display:flex;gap:14px;align-items:center;flex-wrap:wrap;z-index:5;font-size:14px}
.qz .bar button{background:#fff;color:#0b2a4a;border:0;border-radius:6px;padding:5px 10px;cursor:pointer;font-weight:600}
.qz .q{border:1px solid #e3e8ef;border-radius:10px;padding:12px 14px;margin:12px 0;background:#f9fbfd}
.qz .q.ok{border-color:#2f9e44;background:#f2fbf4}.qz .q.ko{border-color:#e03131;background:#fff6f6}
.qz .qn{font-weight:700;color:#0b2a4a}.qz .topic{display:inline-block;font-size:11px;background:#e7f1ff;color:#1c5fb8;
  border-radius:999px;padding:1px 8px;margin-left:6px;font-weight:600}
.qz .multi{display:inline-block;font-size:11px;background:#fff0e6;color:#b84a09;border-radius:999px;padding:1px 8px;margin-left:6px;font-weight:600}
.qz .text{margin:6px 0 8px 0;font-size:14.5px;line-height:1.45}
.qz pre{background:#1e2430;color:#e6edf3;border-radius:8px;padding:10px 12px;font-size:12.5px;overflow:auto;white-space:pre}
.qz code{background:#eef2f7;border-radius:4px;padding:1px 5px;font-size:12.5px;color:#a61e4d}
.qz label{display:block;padding:7px 10px;margin:4px 0;border:1px solid #d6dde8;border-radius:8px;cursor:pointer;background:#fff;font-size:14px}
.qz label:hover{border-color:#1c7ed6}
.qz label.correct{border-color:#2f9e44;background:#e6f7ea}
.qz label.wrong{border-color:#e03131;background:#ffe8e8}
.qz input{margin-right:8px}
.qz .check{margin-top:6px;background:#1c7ed6;color:#fff;border:0;border-radius:6px;padding:6px 14px;cursor:pointer;font-weight:600}
.qz .exp{display:none;margin-top:8px;padding:9px 11px;border-left:4px solid #e8590c;background:#fff6ee;border-radius:6px;font-size:13.5px}
.qz .result{display:none;margin-top:14px;padding:12px 14px;border-radius:10px;background:#0b2a4a;color:#fff}
.qz .result table{border-collapse:collapse;margin-top:6px;font-size:13px}
.qz .result td{padding:3px 10px;border-bottom:1px solid #2b4b6f}
.qz .timer{font-weight:700;font-variant-numeric:tabular-nums}.qz .timer.low{color:#ffb4b4}
.qz input:disabled + b{opacity:.8}
</style>
<div class="qz" id="__ID__">
  <h2>__TITLE__</h2>
  <div class="meta">__META__</div>
  <div class="bar"><span class="timer"></span><span class="score">Answered 0 / 0 · Correct 0</span>
    <button class="finish">📊 Show my result</button><button class="reveal">👀 Reveal all answers</button>
    <button class="reset">↺ Reset</button></div>
  <div class="qs"></div>
  <div class="result"></div>
</div>
<script>
(function(){
  const QS = __DATA__;
  const PASS = __PASS__;
  const MODE = "__MODE__";
  const MINUTES = __MINUTES__;
  const EXAM = MODE === "exam";
  let submitted = false, timer = null, left = MINUTES * 60, armed = false;
  const root = document.getElementById("__ID__");
  const esc = s => String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  const md = s => esc(s).replace(/``\s?([\s\S]+?)\s?``/g,(m,c)=>"<code>"+c.replace(/`/g,"&#96;")+"</code>").replace(/`([^`]+)`/g,"<code>$1</code>").replace(/\*\*([^*]+)\*\*/g,"<b>$1</b>").replace(/\n/g,"<br>");
  const state = QS.map(()=>({done:false, ok:false}));
  const box = root.querySelector(".qs");
  QS.forEach((q,i)=>{
    const multi = Array.isArray(q.answer);
    const d = document.createElement("div"); d.className="q";
    let h = `<div><span class="qn">Q${i+1}</span><span class="topic">${esc(q.topic||"")}</span>`+
            (multi?`<span class="multi">Choose ${q.answer.length}</span>`:"")+`</div>`+
            `<div class="text">${md(q.q)}</div>`;
    if(q.code){ h += `<pre>${esc(q.code)}</pre>`; }
    q.options.forEach((o,j)=>{
      h += `<label data-j="${j}"><input type="${multi?"checkbox":"radio"}" name="q${i}_${root.id}" value="${j}">`+
           `<b>${String.fromCharCode(65+j)}.</b> ${md(o)}</label>`;
    });
    h += `<button class="check">Check answer</button><div class="exp"></div>`;
    d.innerHTML = h; box.appendChild(d);
    d.querySelector(".check").onclick = ()=>grade(i, d, false);
  });
  function correctSet(q){ return new Set(Array.isArray(q.answer)?q.answer:[q.answer]); }
  function grade(i, d, reveal){
    const q = QS[i], good = correctSet(q);
    const picked = new Set([...d.querySelectorAll("input:checked")].map(x=>+x.value));
    if(!reveal && picked.size===0){ alert_('Pick an answer first'); return; }
    d.querySelectorAll("label").forEach(l=>{
      const j=+l.dataset.j; l.classList.remove("correct","wrong");
      if(good.has(j)) l.classList.add("correct"); else if(picked.has(j)) l.classList.add("wrong");
    });
    const ok = picked.size===good.size && [...good].every(x=>picked.has(x));
    if(!reveal || picked.size>0){ state[i]={done:true, ok:ok}; d.classList.toggle("ok",ok); d.classList.toggle("ko",!ok); }
    const ans=[...good].map(x=>String.fromCharCode(65+x)).join(", ");
    const e=d.querySelector(".exp"); e.style.display="block";
    e.innerHTML = `<b>${reveal&&picked.size===0?"Answer":(ok?"✅ Correct":"❌ Not quite")} — ${ans}</b><br>${md(q.explanation||"")}`;
    score();
  }
  function alert_(m){ const b=root.querySelector(".score"); const old=b.textContent; b.textContent="⚠️ "+m; setTimeout(score,1200); }
  function answeredCount(){ return [...root.querySelectorAll(".q")].filter(d=>d.querySelector("input:checked")).length; }
  function score(){
    const done=state.filter(s=>s.done).length, ok=state.filter(s=>s.ok).length;
    root.querySelector(".score").textContent = (EXAM && !submitted)
      ? `Answered ${answeredCount()} / ${QS.length}`
      : `Answered ${EXAM ? answeredCount() : done} / ${QS.length} · Correct ${ok}`;
  }
  function tick(){
    const t=root.querySelector(".timer"); if(!EXAM||!MINUTES){ t.style.display="none"; return; }
    const m=Math.floor(Math.max(left,0)/60), s=Math.max(left,0)%60;
    t.textContent=`⏱️ ${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`; t.classList.toggle("low", left<=300);
  }
  function startTimer(){
    if(!EXAM||!MINUTES) { tick(); return; }
    clearInterval(timer); left=MINUTES*60; tick();
    timer=setInterval(()=>{ left--; tick(); if(left<=0){ clearInterval(timer); submitExam(true); } },1000);
  }
  function submitExam(timeUp){
    if(submitted) return; submitted=true; clearInterval(timer);
    root.querySelectorAll(".q").forEach((d,i)=>grade(i,d,true));
    root.querySelectorAll("input").forEach(x=>x.disabled=true);
    root.querySelector(".finish").textContent="📊 Show my result";
    showResult(timeUp);
  }
  root.querySelector(".reveal").onclick=()=>root.querySelectorAll(".q").forEach((d,i)=>grade(i,d,true));
  root.querySelectorAll("input").forEach(x=>x.addEventListener("change", score));
  root.querySelector(".reset").onclick=()=>{
    submitted=false; armed=false;
    if(EXAM) root.querySelector(".finish").textContent="📤 Submit exam";
    root.querySelectorAll("input").forEach(x=>{x.checked=false; x.disabled=false;});
    root.querySelectorAll("label").forEach(l=>l.classList.remove("correct","wrong"));
    root.querySelectorAll(".q").forEach(d=>d.classList.remove("ok","ko"));
    root.querySelectorAll(".exp").forEach(e=>e.style.display="none");
    state.forEach((s,i)=>state[i]={done:false,ok:false}); root.querySelector(".result").style.display="none"; score();
    startTimer();
  };
  root.querySelector(".finish").onclick=()=>{
    if(EXAM && !submitted){
      if(!armed){ armed=true; root.querySelector(".finish").textContent="⚠️ Click again to submit";
                  setTimeout(()=>{ if(!submitted){ armed=false; root.querySelector(".finish").textContent="📤 Submit exam"; } },3000); return; }
      submitExam(false); return;
    }
    showResult(false);
  };
  function showResult(timeUp){
    const topics={}; QS.forEach((q,i)=>{const t=q.topic||"General"; topics[t]=topics[t]||[0,0]; topics[t][1]++; if(state[i].ok) topics[t][0]++;});
    const ok=state.filter(s=>s.ok).length, pct=Math.round(100*ok/QS.length);
    let rows=Object.entries(topics).map(([t,[a,b]])=>`<tr><td>${esc(t)}</td><td>${a} / ${b}</td><td>${Math.round(100*a/b)}%</td></tr>`).join("");
    const r=root.querySelector(".result"); r.style.display="block";
    const unanswered = QS.length - (EXAM ? answeredCount() : state.filter(s=>s.done).length);
    r.innerHTML=(timeUp?"<b>⏰ Time is up — your answers were submitted.</b><br>":"")+
      `<b>Score: ${ok} / ${QS.length} (${pct}%)</b> — ${pct>=PASS*100?"🎉 Pass level reached!":"📚 Review the topics below and retry."}`+
      (unanswered?` <span style="opacity:.8">(${unanswered} unanswered = wrong)</span>`:"")+
      `<table><tr><td><b>Topic</b></td><td><b>Correct</b></td><td></td></tr>${rows}</table>`;
    r.scrollIntoView({behavior:"smooth"});
  }
  if(EXAM){
    root.querySelectorAll(".check").forEach(b=>b.style.display="none");
    root.querySelector(".reveal").style.display="none";
    root.querySelector(".finish").textContent="📤 Submit exam";
  }
  startTimer();
  score();
})();
</script>
"""


def render_quiz(questions, title="Quiz", meta="", pass_mark=0.8, mode="practice", minutes=None):
    """Render an interactive quiz. Unanswered questions count as wrong in the result.
    mode="practice": instant feedback per question.  mode="exam": answers hidden until "Submit exam";
    minutes=N adds a countdown that submits automatically."""
    assert mode in ("practice", "exam"), "mode must be 'practice' or 'exam'"
    for n, q in enumerate(questions, 1):  # fail fast on typos in the question bank
        assert {"q", "options", "answer"} <= set(q), f"Q{n} is missing q/options/answer"
        answers = q["answer"] if isinstance(q["answer"], list) else [q["answer"]]
        assert all(0 <= a < len(q["options"]) for a in answers), f"Q{n}: answer index out of range"
    data = _json.dumps(questions, ensure_ascii=False).replace("</", "<\\/")
    html = (_QUIZ_TEMPLATE.replace("__DATA__", data)
            .replace("__PASS__", str(pass_mark))
            .replace("__MODE__", mode)
            .replace("__MINUTES__", str(int(minutes or 0)))
            .replace("__ID__", "qz" + _uuid.uuid4().hex[:8])
            .replace("__TITLE__", title)
            .replace("__META__", meta or f"{len(questions)} questions · pass mark {int(pass_mark * 100)}%"))
    displayHTML(html)


def print_answer_key(questions):
    """Plain-text answer key (useful for printing / revision)."""
    for n, q in enumerate(questions, 1):
        answers = q["answer"] if isinstance(q["answer"], list) else [q["answer"]]
        letters = ", ".join(chr(65 + a) for a in answers)
        print(f"Q{n:>2} [{q.get('topic', '')}] -> {letters}\n     {q.get('explanation', '')}\n")


print("📝 Quiz engine loaded - use render_quiz(questions, title)")
