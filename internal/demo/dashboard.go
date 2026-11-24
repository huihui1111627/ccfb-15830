package demo

// dashboardHTML 监控页面：原生 JS + 内联 SVG，无任何外部依赖。
// 每个接口展示：并发上限/在途/等待数、基线与当前延迟、被拒请求随时间曲线。
const dashboardHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>寺庙香客服务 · 自适应并发限流监控</title>
<style>
  :root{
    --bg:#0f1420; --panel:#171f2e; --panel2:#1e293d; --line:#2a3850;
    --txt:#e6edf6; --sub:#8da2bd; --gold:#e0b25a; --red:#e06a6a;
    --green:#5fce8f; --blue:#5aa9e6; --purple:#b18ce0; --amber:#e0a05a;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
    font-family:-apple-system,"PingFang SC","Microsoft YaHei",monospace,sans-serif;font-size:13px}
  header{padding:14px 22px;background:linear-gradient(90deg,#1a2436,#131a28);
    border-bottom:1px solid var(--line);display:flex;align-items:center;gap:18px;flex-wrap:wrap}
  header h1{font-size:17px;margin:0;color:var(--gold);font-weight:600;letter-spacing:1px}
  .ctrl{display:flex;gap:8px;align-items:center}
  button{background:var(--panel2);color:var(--txt);border:1px solid var(--line);
    border-radius:6px;padding:6px 14px;cursor:pointer;font-size:13px}
  button:hover{border-color:var(--gold)}
  button.active{background:var(--gold);color:#1a1408;border-color:var(--gold);font-weight:600}
  .badge{padding:4px 10px;border-radius:12px;background:var(--panel2);border:1px solid var(--line);color:var(--sub)}
  .badge.on{background:#5a2a2a;color:#ffb0b0;border-color:#7a3a3a}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(520px,1fr));gap:16px;padding:18px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
  .card h2{font-size:14px;margin:0 0 10px;display:flex;justify-content:space-between;align-items:center}
  .algo-tag{font-size:11px;color:var(--blue);border:1px solid var(--line);padding:2px 8px;border-radius:10px}
  .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}
  .stat{background:var(--panel2);border-radius:8px;padding:8px 10px}
  .stat .k{color:var(--sub);font-size:11px}
  .stat .v{font-size:18px;font-weight:600;margin-top:2px}
  .chart-wrap{margin-top:10px}
  .chart-title{color:var(--sub);font-size:11px;margin:8px 0 4px;display:flex;gap:14px;flex-wrap:wrap}
  .lg{display:inline-flex;align-items:center;gap:4px}
  .dot{width:9px;height:9px;border-radius:2px;display:inline-block}
  svg{width:100%;height:130px;display:block;background:#0d1320;border-radius:6px}
  .prio-row{display:flex;gap:8px;margin-top:8px;font-size:11px;color:var(--sub)}
  .prio{flex:1;background:var(--panel2);border-radius:6px;padding:6px 8px;text-align:center}
  .prio b{display:block;font-size:14px;color:var(--txt);margin-top:2px}
  .probe{color:var(--amber)}
</style>
</head>
<body>
<header>
  <h1>🏯 寺庙香客服务 · 自适应并发限流</h1>
  <div class="ctrl">
    <span class="badge" id="algoBadge">算法：-</span>
    <button id="btnVegas" onclick="setAlgo('vegas')">Vegas 基线比</button>
    <button id="btnGradient" onclick="setAlgo('gradient')">Gradient 梯度</button>
  </div>
  <div class="ctrl">
    <span class="badge" id="festBadge">日常模式</span>
    <button id="btnFest" onclick="toggleFestival()">切换节庆（十几倍流量 + 容量突降）</button>
  </div>
  <span class="badge" id="clock"></span>
</header>
<div class="grid" id="grid"></div>

<script>
const PRIO_NAMES=['低档','中档','高档'];
const PRIO_COLOR=['#8da2bd','#5aa9e6','#e0b25a'];
let state=null;

function fmt(n){return (n==null)?'-':Number(n).toLocaleString();}
function f1(n){return n==null?'-':(+n).toFixed(1);}

// 通用 SVG 折线图：series=[{name,color,points:[{x,y}]}]，x/y 自动归一化
function lineChart(el, series, yLabel){
  const W=el.clientWidth||500, H=130, padL=34, padR=8, padT=10, padB=16;
  const iw=W-padL-padR, ih=H-padT-padB;
  let allX=[], allY=[];
  series.forEach(s=>s.points.forEach(p=>{allX.push(p.x);allY.push(p.y);}));
  if(allX.length===0){el.innerHTML='<text x="10" y="20" fill="#8da2bd">采集中…</text>';return;}
  const xmin=Math.min(...allX), xmax=Math.max(...allX)||xmin+1;
  let ymax=Math.max(...allY,1); ymax*=1.08;
  const X=x=>padL+(xmax===xmin?0:(x-xmin)/(xmax-xmin)*iw);
  const Y=y=>padT+ih-(y/ymax)*ih;
  let svg='<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none">';
  // 网格 + y 轴刻度
  for(let g=0;g<=4;g++){const yv=ymax*g/4, yy=Y(yv);
    svg+='<line x1="'+padL+'" y1="'+yy+'" x2="'+(W-padR)+'" y2="'+yy+'" stroke="#1c2740" stroke-width="1"/>';
    svg+='<text x="'+(padL-4)+'" y="'+(yy+3)+'" fill="#5f7393" font-size="9" text-anchor="end">'+Math.round(yv)+'</text>';}
  series.forEach(s=>{
    let d='';
    s.points.forEach((p,i)=>{d+=(i?'L':'M')+X(p.x).toFixed(1)+' '+Y(p.y).toFixed(1);});
    svg+='<path d="'+d+'" fill="none" stroke="'+s.color+'" stroke-width="1.6" '+(s.dash?'stroke-dasharray="4 3"':'')+'/>';
  });
  svg+='</svg>';
  el.innerHTML=svg;
}

function legend(series){
  return '<div class="chart-title">'+series.map(s=>
    '<span class="lg"><span class="dot" style="background:'+s.color+'"></span>'+s.name+'</span>').join('')+'</div>';
}

function renderEndpoint(ep){
  const h=ep.history||[];
  // 图1：并发上限 + 实际在途 + 真实容量
  const s1=[
    {name:'并发上限',color:'#5aa9e6',points:h.map(p=>({x:p.t,y:p.limit}))},
    {name:'实际在途',color:'#5fce8f',points:h.map(p=>({x:p.t,y:p.inFlight}))},
    {name:'等待数',color:'#b18ce0',points:h.map(p=>({x:p.t,y:p.waiters}))},
    {name:'下游真实容量',color:'#e06a6a',dash:true,points:h.map(p=>({x:p.t,y:p.trueCapacity}))},
  ];
  // 图2：最小基线 + 当前延迟 + 排队比（右轴语义在标题说明）
  const s2=[
    {name:'最小延迟基线ms',color:'#8da2bd',points:h.map(p=>({x:p.t,y:p.baselineMs}))},
    {name:'当前延迟p50 ms',color:'#e0b25a',points:h.map(p=>({x:p.t,y:p.currentRttMs}))},
  ];
  const s2b=[
    {name:'排队比 当前/基线',color:'#e0a05a',points:h.map(p=>({x:p.t,y:p.queueRatio}))},
  ];
  // 图3：累计被拒请求（按总量，展示增长曲线）
  const s3=[{name:'累计被拒请求',color:'#e06a6a',points:h.map(p=>({x:p.t,y:p.rejected}))}];
  const s4=[{name:'累计到达',color:'#5aa9e6',points:h.map(p=>({x:p.t,y:p.arrived}))},
             {name:'累计成功',color:'#5fce8f',points:h.map(p=>({x:p.t,y:p.success}))}];

  const totalRej=ep.rejected.reduce((a,b)=>a+b,0);
  const totalArr=ep.arrived.reduce((a,b)=>a+b,0);
  const rejRate=totalArr? (100*totalRej/totalArr):0;

  let prioHtml='';
  for(let p=0;p<3;p++){
    const rate=ep.arrived[p]?(100*ep.succeeded[p]/ep.arrived[p]):0;
    prioHtml+='<div class="prio" style="border-top:2px solid '+PRIO_COLOR[p]+'">'+PRIO_NAMES[p]+
      '<b>'+rate.toFixed(1)+'%</b>成功 '+fmt(ep.succeeded[p])+' / 拒 '+fmt(ep.rejected[p])+'</div>';
  }

  return '<div class="card">'
    +'<h2><span>'+ep.name+'</span><span class="algo-tag">'+ep.algo+(ep.probing?' · <span class="probe">探底中</span>':'')+'</span></h2>'
    +'<div class="stats">'
    +'<div class="stat"><div class="k">并发上限 / 真实容量</div><div class="v">'+ep.limit+' / '+ep.trueCapacity+'</div></div>'
    +'<div class="stat"><div class="k">实际在途 / 等待</div><div class="v">'+ep.inFlight+' / '+ep.waiters+'</div></div>'
    +'<div class="stat"><div class="k">基线 / 当前延迟(ms)</div><div class="v">'+f1(ep.baselineMs)+' / '+f1(ep.currentRttMs)+'</div></div>'
    +'<div class="stat"><div class="k">排队比 / 强信号</div><div class="v">'+f1(ep.queueRatio)+' / '+ep.strongEvents+'</div></div>'
    +'</div>'
    +'<div class="prio-row">'+prioHtml+'</div>'
    +'<div class="chart-wrap">'+legend(s1)+'<svg id="c1-'+ep.key+'"></svg></div>'
    +'<div class="chart-wrap">'+legend(s2.concat(s2b))+'<svg id="c2-'+ep.key+'"></svg></div>'
    +'<div class="chart-wrap">'+legend(s3.concat(s4))+'<svg id="c3-'+ep.key+'"></svg></div>'
    +'<div class="prio-row"><div class="prio">总拒绝率 <b>'+rejRate.toFixed(1)+'%</b></div>'
    +'<div class="prio">本地排队p50/p99 <b>'+f1(ep.waitP50Ms)+'/'+f1(ep.waitP99Ms)+'ms</b></div>'
    +'<div class="prio">等待超时拒绝 <b>'+fmt(ep.rejWaitTimeout.reduce((a,b)=>a+b,0))+'</b></div></div>'
    +'</div>';
}

function render(){
  if(!state)return;
  document.getElementById('grid').innerHTML=state.endpoints.map(renderEndpoint).join('');
  state.endpoints.forEach(ep=>{
    const h=ep.history||[];
    lineChart(document.getElementById('c1-'+ep.key),[
      {name:'上限',color:'#5aa9e6',points:h.map(p=>({x:p.t,y:p.limit}))},
      {name:'在途',color:'#5fce8f',points:h.map(p=>({x:p.t,y:p.inFlight}))},
      {name:'等待',color:'#b18ce0',points:h.map(p=>({x:p.t,y:p.waiters}))},
      {name:'真实容量',color:'#e06a6a',dash:true,points:h.map(p=>({x:p.t,y:p.trueCapacity}))},
    ]);
    lineChart(document.getElementById('c2-'+ep.key),[
      {name:'基线ms',color:'#8da2bd',points:h.map(p=>({x:p.t,y:p.baselineMs}))},
      {name:'当前p50ms',color:'#e0b25a',points:h.map(p=>({x:p.t,y:p.currentRttMs}))},
      {name:'排队比',color:'#e0a05a',points:h.map(p=>({x:p.t,y:p.queueRatio}))},
    ]);
    lineChart(document.getElementById('c3-'+ep.key),[
      {name:'累计被拒',color:'#e06a6a',points:h.map(p=>({x:p.t,y:p.rejected}))},
      {name:'累计到达',color:'#5aa9e6',points:h.map(p=>({x:p.t,y:p.arrived}))},
      {name:'累计成功',color:'#5fce8f',points:h.map(p=>({x:p.t,y:p.success}))},
    ]);
  });
  document.getElementById('algoBadge').textContent='算法：'+(state.algo==='gradient'?'Gradient 梯度':'Vegas 基线比');
  document.getElementById('btnVegas').classList.toggle('active',state.algo==='vegas');
  document.getElementById('btnGradient').classList.toggle('active',state.algo==='gradient');
  const fb=document.getElementById('festBadge');
  fb.textContent=state.festival?'节庆模式（高负载）':'日常模式';
  fb.classList.toggle('on',state.festival);
  document.getElementById('btnFest').classList.toggle('active',state.festival);
  document.getElementById('clock').textContent=new Date().toLocaleTimeString();
}

async function refresh(){
  try{const r=await fetch('/api/state');state=await r.json();render();}
  catch(e){/* 瞬时网络错误忽略，下一轮刷新 */}
}
async function setAlgo(a){await fetch('/api/algo?algo='+a);refresh();}
async function toggleFestival(){
  const on=!(state&&state.festival);
  await fetch('/api/festival?on='+(on?'1':'0'));refresh();
}
refresh();
setInterval(refresh,1000);
</script>
</body>
</html>`
